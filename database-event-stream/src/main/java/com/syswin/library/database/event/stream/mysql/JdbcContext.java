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

import com.github.shyiko.mysql.binlog.GtidSet;
import com.syswin.library.database.event.stream.DbEventStreamConnectionException;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcContext {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final DataSource dataSource;

  JdbcContext(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  GtidSet purgedGtidSet() {
    AtomicReference<String> gtidSet = new AtomicReference<>();

    query("SHOW GLOBAL VARIABLES LIKE 'gtid_purged'", rs -> {
      if (rs.next() && rs.getMetaData().getColumnCount() > 1) {
        gtidSet.set(rs.getString(2)); // GTID set, may be null, blank, or contain a GTID set
      }
    });

    return toGtidSet(gtidSet);
  }

  private void query(String sql, ResultSetConsumer consumer) {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        log.debug("Executing sql statement {} on data source {}", sql, connection.getMetaData().getURL());
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          consumer.accept(resultSet);
        }
      }
    } catch (SQLException e) {
      throw new DbEventStreamConnectionException("Failed to execute sql: " + sql, e);
    }
  }

  private GtidSet toGtidSet(AtomicReference<String> gtidSet) {
    String gtidStr = gtidSet.get();
    return gtidStr == null ? new GtidSet("") : new GtidSet(gtidStr);
  }

  private interface ResultSetConsumer {

    void accept(ResultSet rs) throws SQLException;
  }

}
