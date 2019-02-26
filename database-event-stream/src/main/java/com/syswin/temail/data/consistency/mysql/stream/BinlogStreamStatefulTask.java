package com.syswin.temail.data.consistency.mysql.stream;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import java.io.IOException;
import java.util.function.Consumer;

public class BinlogStreamStatefulTask implements StatefulTask {

  private final MysqlBinLogStream binLogStream;
  private final Consumer<Event> mysqlEventHandler;
  private final EventType[] eventTypes;

  public BinlogStreamStatefulTask(MysqlBinLogStream binLogStream,
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
    } catch (IOException e) {
      errorHandler.accept(e);
    }
  }

  @Override
  public void stop() {
    binLogStream.stop();
  }
}
