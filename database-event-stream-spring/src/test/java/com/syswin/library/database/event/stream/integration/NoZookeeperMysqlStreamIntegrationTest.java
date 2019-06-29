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

package com.syswin.library.database.event.stream.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.library.database.event.stream.integration.containers.MysqlContainer;
import java.util.Queue;
import javax.sql.DataSource;
import org.awaitility.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestApplication.class, StatefulTaskConfig.class}, properties = {
    "spring.datasource.username=root",
    "spring.datasource.password=password",
    "library.database.stream.election.enabled=false",
    "library.database.stream.update.mode=database"
})
@ActiveProfiles("one-source")
public class NoZookeeperMysqlStreamIntegrationTest {

  @ClassRule
  public static final MysqlContainer mysql = new MysqlContainer()
      .withNetworkAliases("mysql-temail")
      .withEnv("MYSQL_DATABASE", "consistency")
      .withEnv("MYSQL_USER", "syswin")
      .withEnv("MYSQL_PASSWORD", "password")
      .withEnv("MYSQL_ROOT_PASSWORD", "password");

  private final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();

  @Autowired
  private Queue<String> handledEvents;

  @Autowired
  private DataSource dataSource;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("spring.datasource.url",
        "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(3306) + "/consistency?useSSL=false");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.datasource.url");
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
  }
}
