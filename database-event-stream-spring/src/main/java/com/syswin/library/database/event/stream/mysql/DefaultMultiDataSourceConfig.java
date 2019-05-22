package com.syswin.library.database.event.stream.mysql;

import static com.syswin.library.stateful.task.runner.zookeeper.ZookeeperPaths.ZK_ROOT_PATH;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import com.syswin.library.database.event.stream.CounterBinlogSyncRecorder;
import com.syswin.library.database.event.stream.MultiDataSourceConfig;
import com.syswin.library.database.event.stream.MultiDataSourceConfig.DbStreamContext;
import com.syswin.library.database.event.stream.MultiDataSourceConfig.DbStreamDataSource;
import com.syswin.library.database.event.stream.zookeeper.AsyncZkBinlogSyncRecorder;
import com.syswin.library.database.event.stream.zookeeper.BlockingZkBinlogSyncRecorder;
import com.syswin.library.stateful.task.runner.StatefulTask;
import com.syswin.library.stateful.task.runner.zookeeper.ZkBasedStatefulTaskRunner;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.database.stream.multi.enabled", havingValue = "true")
@Configuration
@EnableConfigurationProperties(MultiDataSourceConfig.class)
class DefaultMultiDataSourceConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Random random = new Random(System.currentTimeMillis());
  private final MultiDataSourceConfig multiDataSourceConfig;
  private final List<BinlogSyncRecorder> binlogSyncRecorders;
  private final List<DataSource> dataSources;
  private final List<ZkBasedStatefulTaskRunner> taskRunners;

  private final StatefulTaskComposer statefulTaskComposer;

  DefaultMultiDataSourceConfig(MultiDataSourceConfig multiDataSourceConfig,
      StatefulTaskComposer statefulTaskComposer,
      List<DataSource> dataSources) {

    this.multiDataSourceConfig = multiDataSourceConfig;
    this.taskRunners = new ArrayList<>(multiDataSourceConfig.getContexts().size());
    this.binlogSyncRecorders = new ArrayList<>(multiDataSourceConfig.getContexts().size());
    this.dataSources = dataSources;
    this.statefulTaskComposer = statefulTaskComposer;
  }

  private BinlogSyncRecorder binlogSyncRecorder(CuratorFramework curator,
      String clusterRoot,
      String clusterName,
      String updateMode,
      long updateIntervalMillis) {
    BinlogSyncRecorder recorder;

    if ("blocking".equalsIgnoreCase(updateMode)) {
      log.info("Starting with blocking binlog recorder for cluster {}", clusterName);
      recorder = new CounterBinlogSyncRecorder(new BlockingZkBinlogSyncRecorder(clusterRoot, clusterName, curator));
    } else {
      log.info("Starting with async binlog recorder for cluster {}", clusterName);
      recorder = new CounterBinlogSyncRecorder(new AsyncZkBinlogSyncRecorder(clusterRoot, clusterName, curator, updateIntervalMillis));
    }

    binlogSyncRecorders.add(recorder);
    recorder.start();
    return recorder;
  }

  private BinlogStreamStatefulTask binLogStreamTask(
      DataSource dataSource,
      String username,
      String password,
      long serverId,
      EventType[] eventTypes,
      Consumer<Event> eventHandler,
      BinlogSyncRecorder binlogSyncRecorder) throws SQLException {

    String[] databaseUrl;
    try (Connection connection = dataSource.getConnection()) {
      databaseUrl = connection.getMetaData().getURL()
          .replaceFirst("^.*//", "")
          .replaceFirst("/.*$", "")
          .split(":");
    }

    serverId = serverId == 0 ? random.nextInt(Integer.MAX_VALUE) + 1 : serverId;

    return new MysqlBinlogStreamStatefulTaskBuilder()
        .dataSource(dataSource)
        .hostname(databaseUrl[0])
        .port(Integer.parseInt(databaseUrl[1]))
        .username(username)
        .password(password)
        .serverId(serverId)
        .binlogSyncRecorder(binlogSyncRecorder)
        .databaseEventHandler(eventHandler)
        .addEventTypes(eventTypes)
        .build();
  }

  @Bean
  List<ZkBasedStatefulTaskRunner> taskRunner(
      @Value("${library.database.stream.cluster.root:" + ZK_ROOT_PATH + "}") String clusterRoot,
      @Value("${library.database.stream.participant.id}") String participantId,
      @Value("${library.database.stream.mysql.serverId:0}") long serverId,
      @Value("${library.database.stream.update.mode}") String updateMode,
      @Value("${library.database.stream.update.interval:200}") long updateIntervalMillis,
      EventType[] eventTypes,
      Function<DataSource, Consumer<Event>> eventHandlerSupplier,
      CuratorFramework curator) throws Exception {

    for (DbStreamContext context : multiDataSourceConfig.getContexts()) {
      DataSource dataSource = dataSource(context.getDataSource());
      StatefulTask statefulTask = binLogStreamTask(dataSource,
          context.getDataSource().getUsername(),
          context.getDataSource().getPassword(),
          serverId,
          eventTypes,
          eventHandlerSupplier.apply(dataSource),
          binlogSyncRecorder(curator, clusterRoot, context.getCluster().getName(), updateMode, updateIntervalMillis)
      );

      statefulTask = statefulTaskComposer.apply(dataSource, statefulTask);

      ZkBasedStatefulTaskRunner taskRunner = new ZkBasedStatefulTaskRunner(
          clusterRoot,
          context.getCluster().getName(),
          participantId,
          statefulTask,
          curator);

      taskRunners.add(taskRunner);
      taskRunner.start();
    }
    return taskRunners;
  }

  private DataSource dataSource(DbStreamDataSource dbStreamDataSource) {
    DataSource dataSource = DataSourceBuilder.create()
        .url(dbStreamDataSource.getUrl())
        .username(dbStreamDataSource.getUsername())
        .password(dbStreamDataSource.getPassword())
        .driverClassName("com.mysql.jdbc.Driver")
        .build();

    dataSources.add(dataSource);
    return dataSource;
  }

  @PreDestroy
  void shutdown() {
    for (ZkBasedStatefulTaskRunner taskRunner : taskRunners) {
      taskRunner.shutdown();
    }

    for (BinlogSyncRecorder recorder : binlogSyncRecorders) {
      recorder.shutdown();
    }
  }
}
