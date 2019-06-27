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

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import javax.sql.DataSource;

public class MysqlBinlogStreamStatefulTaskBuilder {

  private final List<EventType> eventTypes = new ArrayList<>();
  private String username;
  private String password;
  private long serverId;
  private BinlogSyncRecorder binlogSyncRecorder;
  private String hostname;
  private int port;
  private Consumer<Event> mysqlEventHandler;
  private DataSource dataSource;

  public MysqlBinlogStreamStatefulTaskBuilder dataSource(DataSource dataSource) {
    this.dataSource = dataSource;
    return this;
  }

  public MysqlBinlogStreamStatefulTaskBuilder username(String username) {
    this.username = username;
    return this;
  }

  public MysqlBinlogStreamStatefulTaskBuilder password(String password) {
    this.password = password;
    return this;
  }

  public MysqlBinlogStreamStatefulTaskBuilder serverId(long serverId) {
    this.serverId = serverId;
    return this;
  }

  public MysqlBinlogStreamStatefulTaskBuilder binlogSyncRecorder(BinlogSyncRecorder binlogSyncRecorder) {
    this.binlogSyncRecorder = binlogSyncRecorder;
    return this;
  }

  public MysqlBinlogStreamStatefulTaskBuilder hostname(String hostname) {
    this.hostname = hostname;
    return this;
  }

  public MysqlBinlogStreamStatefulTaskBuilder port(int port) {
    this.port = port;
    return this;
  }

  public MysqlBinlogStreamStatefulTaskBuilder databaseEventHandler(Consumer<Event> mysqlEventHandler) {
    this.mysqlEventHandler = mysqlEventHandler;
    return this;
  }

  public MysqlBinlogStreamStatefulTaskBuilder addEventTypes(EventType... eventTypes) {
    Collections.addAll(this.eventTypes, eventTypes);
    return this;
  }

  public BinlogStreamStatefulTask build() {
    MysqlBinLogStream binLogStream = new MysqlBinLogStream(
        hostname,
        port,
        username,
        password,
        serverId,
        new JdbcContext(dataSource),
        binlogSyncRecorder);

    return new BinlogStreamStatefulTask(binLogStream, mysqlEventHandler, eventTypes.toArray(new EventType[0]));
  }
}
