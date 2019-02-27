package com.syswin.library.database.event.stream.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.syswin.library.database.event.stream.DbEventStreamConnectionException;
import com.syswin.library.stateful.task.runner.StatefulTask;
import java.io.IOException;
import java.util.function.Consumer;

class BinlogStreamStatefulTask implements StatefulTask {

  private final MysqlBinLogStream binLogStream;
  private final Consumer<Event> mysqlEventHandler;
  private final EventType[] eventTypes;

  BinlogStreamStatefulTask(MysqlBinLogStream binLogStream,
      Consumer<Event> mysqlEventHandler,
      EventType... eventTypes) {
    this.binLogStream = binLogStream;
    this.mysqlEventHandler = mysqlEventHandler;
    this.eventTypes = eventTypes;
  }

  @Override
  public void start(Consumer<Throwable> errorHandler) {
    try {
      binLogStream.start(mysqlEventHandler, errorHandler, eventTypes);
    } catch (IOException | DbEventStreamConnectionException e) {
      errorHandler.accept(e);
    }
  }

  @Override
  public void stop() {
    binLogStream.stop();
  }
}
