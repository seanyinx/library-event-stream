package com.syswin.library.database.event.stream.mysql;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.syswin.library.database.event.stream.DbEventStreamConnectionException;
import java.lang.invoke.MethodHandles;
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

  GtidSet availableGtidSet() {
    AtomicReference<String> gtidSet = new AtomicReference<>();

    query("SHOW MASTER STATUS", rs -> {
      if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
        gtidSet.set(rs.getString(5)); // GTID set, may be null, blank, or contain a GTID set
      }
    });

    return toGtidSet(gtidSet);
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
    try (Statement statement = dataSource.getConnection().createStatement()) {
      log.debug("Executing sql statement {} on data source {}", sql, dataSource.getConnection().getMetaData().getURL());
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        consumer.accept(resultSet);
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
