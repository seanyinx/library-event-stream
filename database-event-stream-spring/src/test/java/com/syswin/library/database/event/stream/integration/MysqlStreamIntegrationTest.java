package com.syswin.library.database.event.stream.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.library.database.event.stream.integration.StatefulTaskConfig.StoppableStatefulTask;
import com.syswin.library.database.event.stream.integration.containers.MysqlContainer;
import com.syswin.library.database.event.stream.integration.containers.ZookeeperContainer;
import com.syswin.library.stateful.task.runner.StatefulTask;
import java.util.Queue;
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

  @Autowired
  private Queue<String> handledEvents;

  @Autowired
  private DataSource dataSource;

  @Autowired
  private StatefulTask mysqlBinLogStream;

  @Autowired
  private StoppableStatefulTask statefulTask;

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
    databasePopulator.addScript(new ClassPathResource("data.sql"));
  }

  @Test
  public void streamEventsToMq() {

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
  }
}
