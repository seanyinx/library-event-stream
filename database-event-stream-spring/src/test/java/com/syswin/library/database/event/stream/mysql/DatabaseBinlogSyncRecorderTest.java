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

import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import com.syswin.library.database.event.stream.DatabaseBinlogSyncRecorder;
import com.syswin.library.database.event.stream.integration.TestApplication;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestApplication.class})
@ActiveProfiles("binlog-db")
public class DatabaseBinlogSyncRecorderTest {

  private static final int UPDATE_INTERVAL_MILLIS = 100;

  private final String position = uniquify("position");
  private final String clusterName = uniquify("cluster");
  private final Map<String, String> positions = new HashMap<>();

  @Autowired
  private DataSource dataSource;

  private BinlogSyncRecorder recorder;

  @Before
  public void setUp() {
    recorder = new DatabaseBinlogSyncRecorder(clusterName, dataSource, UPDATE_INTERVAL_MILLIS);
    recorder.start();
  }

  @After
  public void tearDown() {
    recorder.shutdown();
    DatabasePopulatorUtils.execute(new ResourceDatabasePopulator(new ClassPathResource("sql/cleanup.sql")), dataSource);
  }

  @Test
  public void savesBinlogPositionInDb() throws SQLException {
    recorder.record(position);
    recorder.flush();

    retrievePositionFromDb(positions);

    assertThat(positions).hasSize(1);
    assertThat(positions.get(clusterName)).isEqualTo(position);


    String anotherCluster = uniquify("another-cluster");
    String anotherPosition = uniquify("another-position");
    BinlogSyncRecorder recorder = new DatabaseBinlogSyncRecorder(anotherCluster, dataSource, UPDATE_INTERVAL_MILLIS);
    recorder.record(anotherPosition);
    recorder.flush();

    retrievePositionFromDb(positions);

    assertThat(positions).hasSize(2);
    assertThat(positions.get(clusterName)).isEqualTo(position);
    assertThat(positions.get(anotherCluster)).isEqualTo(anotherPosition);
  }

  @Test
  public void updatesBinlogPositionInDb() throws SQLException {
    recorder.record(position);
    recorder.flush();

    String anotherPosition = uniquify("another-position");
    recorder.record(anotherPosition);
    recorder.flush();

    retrievePositionFromDb(positions);

    assertThat(positions).hasSize(1);
    assertThat(positions.get(clusterName)).isEqualTo(anotherPosition);
  }

  @Test
  public void getBinlogPositionInDb() {
    assertThat(recorder.position()).isEmpty();

    recorder.record(position);
    recorder.flush();

    assertThat(recorder.position()).isEqualTo(position);
  }

  private void retrievePositionFromDb(Map<String, String> positions) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement("select cluster_name, binlog_position from library_event_stream")) {
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            positions.put(resultSet.getString(1), resultSet.getString(2));
          }
        }
      }
    }
  }
}
