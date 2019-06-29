/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.library.database.event.stream.mysql;

import static com.syswin.library.stateful.task.runner.zookeeper.ZookeeperPaths.ZK_ROOT_PATH;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import com.syswin.library.database.event.stream.CounterBinlogSyncRecorder;
import com.syswin.library.database.event.stream.DatabaseBinlogSyncRecorder;
import com.syswin.library.database.event.stream.zookeeper.AsyncZkBinlogSyncRecorder;
import com.syswin.library.database.event.stream.zookeeper.BlockingZkBinlogSyncRecorder;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
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

  @Value("${library.database.stream.cluster.root:" + ZK_ROOT_PATH + "}")
  private String clusterRoot;

  @Value("${library.database.stream.cluster.name}")
  private String clusterName;

  @ConditionalOnProperty(value = "library.database.stream.update.mode", havingValue = "async", matchIfMissing = true)
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  BinlogSyncRecorder asyncBinlogSyncRecorder(CuratorFramework curator,
      @Value("${library.database.stream.update.interval:200}") long updateIntervalMillis) {
    log.info("Starting with async binlog recorder");
    return new CounterBinlogSyncRecorder(new AsyncZkBinlogSyncRecorder(clusterRoot, clusterName, curator, updateIntervalMillis));
  }

  @ConditionalOnProperty(value = "library.database.stream.update.mode", havingValue = "blocking")
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  BinlogSyncRecorder blockingBinlogSyncRecorder(CuratorFramework curator) {
    log.info("Starting with blocking binlog recorder");
    return new CounterBinlogSyncRecorder(new BlockingZkBinlogSyncRecorder(clusterRoot, clusterName, curator));
  }

  @ConditionalOnProperty(value = "library.database.stream.update.mode", havingValue = "database")
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  BinlogSyncRecorder databaseBinlogSyncRecorder(
      @Value("${library.database.stream.update.interval:200}") long updateIntervalMillis,
      DataSource dataSource) {
    log.info("Starting with database based binlog recorder");
    return new CounterBinlogSyncRecorder(new DatabaseBinlogSyncRecorder(clusterName, dataSource, updateIntervalMillis));
  }

  @ConditionalOnMissingBean
  @Bean
  BinlogStreamStatefulTask binLogStreamTask(
      DataSource dataSource,
      @Value("${spring.datasource.username}") String username,
      @Value("${spring.datasource.password}") String password,
      @Value("${library.database.stream.mysql.serverId:0}") long serverId,
      EventType[] eventTypes,
      Function<DataSource, Consumer<Event>> eventHandler,
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
        .databaseEventHandler(eventHandler.apply(dataSource))
        .addEventTypes(eventTypes)
        .build();
  }
}
