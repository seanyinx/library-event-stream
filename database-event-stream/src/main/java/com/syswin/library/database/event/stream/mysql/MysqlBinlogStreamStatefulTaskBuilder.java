package com.syswin.library.database.event.stream.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class MysqlBinlogStreamStatefulTaskBuilder {

  private final List<EventType> eventTypes = new ArrayList<>();
  private String username;
  private String password;
  private long serverId;
  private BinlogSyncRecorder binlogSyncRecorder;
  private String hostname;
  private int port;
  private Consumer<Event> mysqlEventHandler;

  public MysqlBinlogStreamStatefulTaskBuilder() {
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
        binlogSyncRecorder);

    return new BinlogStreamStatefulTask(binLogStream, mysqlEventHandler, eventTypes.toArray(new EventType[0]));
  }
}
