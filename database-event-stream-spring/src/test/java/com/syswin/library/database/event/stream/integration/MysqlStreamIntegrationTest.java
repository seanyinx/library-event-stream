package com.syswin.library.database.event.stream.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import com.syswin.library.stateful.task.runner.StatefulTask;
import com.syswin.library.database.event.stream.integration.StatefulTaskConfig.StoppableStatefulTask;
import com.syswin.library.database.event.stream.integration.containers.MysqlContainer;
import com.syswin.library.database.event.stream.integration.containers.ZookeeperContainer;
import java.util.List;
import javax.sql.DataSource;
import org.awaitility.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.Network;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestApplication.class, StatefulTaskConfig.class}, properties = {
    "spring.datasource.username=root",
    "spring.datasource.password=password"
})
@ActiveProfiles("one-source")
public class MysqlStreamIntegrationTest {

  private static final Network NETWORK = Network.newNetwork();

  private static final MysqlContainer mysql = new MysqlContainer().withNetwork(NETWORK)
      .withNetworkAliases("mysql-temail")
      .withEnv("MYSQL_DATABASE", "consistency")
      .withEnv("MYSQL_USER", "syswin")
      .withEnv("MYSQL_PASSWORD", "password")
      .withEnv("MYSQL_ROOT_PASSWORD", "password");

  private static final ZookeeperContainer zookeeper = new ZookeeperContainer()
      .withNetwork(NETWORK)
      .withNetworkAliases("zookeeper-temail");

  @ClassRule
  public static final RuleChain RULES = RuleChain.outerRule(mysql)
      .around(zookeeper);

  private final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();

  private List<String> handledEvents;

  @Autowired
  private MysqlEventHandler eventHandler;

  @Autowired
  private DataSource dataSource;

  @Autowired
  private StatefulTask mysqlBinLogStream;

  @Autowired
  private StoppableStatefulTask statefulTask;

  @Autowired
  private BinlogSyncRecorder recorder;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("spring.datasource.url",
        "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(3306) + "/consistency?useSSL=false");

    System.setProperty("library.database.stream.zk.address",
        zookeeper.getContainerIpAddress() + ":" + zookeeper.getMappedPort(2181));
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.datasource.url");
    System.clearProperty("library.database.stream.zk.address");
  }

  @Before
  public void setUp() {
    handledEvents = eventHandler.events();
    databasePopulator.addScript(new ClassPathResource("data.sql"));
  }

  @Test
  public void streamEventsToMq() throws Exception {

    waitAtMost(Duration.ONE_MINUTE).untilAsserted(() -> assertThat(handledEvents).hasSize(5));
    assertThat(handledEvents).containsExactly(
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john"
    );

    // simulate network interruption
    mysqlBinLogStream.stop();
    DatabasePopulatorUtils.execute(databasePopulator, dataSource);

    // resume for last known position
    waitAtMost(Duration.ONE_SECOND).untilAsserted(() -> assertThat(handledEvents).hasSize(10));
    assertThat(handledEvents).containsExactly(
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john",
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john"
    );

    statefulTask.pause();
    mysqlBinLogStream.stop();
    DatabasePopulatorUtils.execute(databasePopulator, dataSource);
    DatabasePopulatorUtils.execute(databasePopulator, dataSource);

    fastForwardGTID(5);

    statefulTask.resume();
    Thread.sleep(1000);

    // we skipped 5 SQL statements by fast forwarding
    // and each data changing statement is a transaction in MySQL by default
    assertThat(handledEvents).hasSize(17);
  }

  private void fastForwardGTID(int statementsToSkip) {
    String position = recorder.position();
    int delimiter = position.indexOf(":");
    String sequenceRange = position.substring(delimiter);
    String uuid = position.substring(0, delimiter + 1);

    long lastSeqOfGTID = Long.parseLong(sequenceRange.substring(sequenceRange.lastIndexOf("-") + 1));
    position = uuid + "1-" + (lastSeqOfGTID + statementsToSkip);

    recorder.record(position);
    recorder.flush();
  }
}
