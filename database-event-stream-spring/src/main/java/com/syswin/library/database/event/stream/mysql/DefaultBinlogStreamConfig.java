package com.syswin.library.database.event.stream.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import com.syswin.library.database.event.stream.CounterBinlogSyncRecorder;
import com.syswin.library.database.event.stream.zookeeper.AsyncZkBinlogSyncRecorder;
import com.syswin.library.database.event.stream.zookeeper.BlockingZkBinlogSyncRecorder;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.database.stream.multi.enabled", havingValue = "false", matchIfMissing = true)
@Configuration
class DefaultBinlogStreamConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Random random = new Random(System.currentTimeMillis());

  @Value("${library.database.stream.cluster.name}")
  private String clusterName;

  @ConditionalOnMissingBean
  @ConditionalOnProperty(value = "library.database.stream.update.mode", havingValue = "async", matchIfMissing = true)
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  BinlogSyncRecorder asyncBinlogSyncRecorder(CuratorFramework curator,
      @Value("${library.database.stream.update.interval:200}") long updateIntervalMillis) {
    log.info("Starting with async binlog recorder");
    return new CounterBinlogSyncRecorder(new AsyncZkBinlogSyncRecorder(clusterName, curator, updateIntervalMillis));
  }

  @ConditionalOnMissingBean
  @ConditionalOnProperty(value = "library.database.stream.update.mode", havingValue = "blocking")
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  BinlogSyncRecorder blockingBinlogSyncRecorder(CuratorFramework curator) {
    log.info("Starting with blocking binlog recorder");
    return new CounterBinlogSyncRecorder(new BlockingZkBinlogSyncRecorder(clusterName, curator));
  }

  @ConditionalOnMissingBean
  @Bean
  BinlogStreamStatefulTask binLogStreamTask(
      DataSource dataSource,
      @Value("${spring.datasource.username}") String username,
      @Value("${spring.datasource.password}") String password,
      @Value("${library.database.stream.mysql.serverId:0}") long serverId,
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
}
