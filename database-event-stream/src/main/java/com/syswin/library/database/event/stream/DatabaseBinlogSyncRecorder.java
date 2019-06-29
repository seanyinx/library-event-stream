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

package com.syswin.library.database.event.stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseBinlogSyncRecorder implements BinlogSyncRecorder {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String SELECT_BINLOG_SQL = "select binlog_position from library_event_stream where cluster_name = ?";
  private static final String UPDATE_BINLOG_SQL = "insert into library_event_stream(cluster_name, binlog_position) values (?, ?) "
      + "on duplicate key update binlog_position = ?";

  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final String clusterName;
  private final DataSource dataSource;
  private final AtomicBoolean updated = new AtomicBoolean();
  private final long updateIntervalMillis;
  private volatile String position;

  public DatabaseBinlogSyncRecorder(String clusterName, DataSource dataSource, long updateIntervalMillis) {
    this.clusterName = clusterName;
    this.dataSource = dataSource;
    this.updateIntervalMillis = updateIntervalMillis;
  }

  @Override
  public void start() {
    scheduledExecutor.scheduleWithFixedDelay(this::flushIfUpdated, updateIntervalMillis, updateIntervalMillis, MILLISECONDS);
  }

  @Override
  public void record(String position) {
    this.position = position;
    updated.set(true);
    log.info("Saved binlog position [{}] locally", position);
  }

  @Override
  public String position() {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(SELECT_BINLOG_SQL)) {
        statement.setString(1, clusterName);
        try (ResultSet resultSet = statement.executeQuery()) {
          if (resultSet.next()) {
            return resultSet.getString(1);
          }
        }
      }
    } catch (SQLException e) {
      throw new DbEventStreamConnectionException(cause("retrieving"), e);
    }

    return "";
  }

  @Override
  public void flush() {
    flushIfUpdated();
  }

  @Override
  public void shutdown() {
    scheduledExecutor.shutdownNow();
  }

  private void flushIfUpdated() {
    if (updated.compareAndSet(true, false)) {
      try (Connection connection = dataSource.getConnection()) {
        try (PreparedStatement statement = connection.prepareStatement(UPDATE_BINLOG_SQL)) {
          statement.setString(1, clusterName);
          statement.setString(2, position);
          statement.setString(3, position);
          statement.execute();
          log.info("Executed statement {}", statement);
        }
      } catch (SQLException e) {
        updated.compareAndSet(false, true);
        throw new DbEventStreamConnectionException(cause("updating"), e);
      }
    }
  }

  private String cause(String operation) {
    return "Unexpected exception when " + operation + " binlog position of cluster " + clusterName;
  }
}
