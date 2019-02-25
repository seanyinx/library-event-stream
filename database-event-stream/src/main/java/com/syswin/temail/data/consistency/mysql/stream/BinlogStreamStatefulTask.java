package com.syswin.temail.data.consistency.mysql.stream;

import com.github.shyiko.mysql.binlog.event.Event;
import java.io.IOException;
import java.util.function.Consumer;

class BinlogStreamStatefulTask implements StatefulTask {

  private final MysqlBinLogStream binLogStream;
  private final Consumer<Event> mysqlEventHandler;

  BinlogStreamStatefulTask(MysqlBinLogStream binLogStream, Consumer<Event> mysqlEventHandler) {
    this.binLogStream = binLogStream;
    this.mysqlEventHandler = mysqlEventHandler;
  }

  @Override
  public void start(Consumer<Throwable> errorHandler) {
    try {
      binLogStream.start(mysqlEventHandler, errorHandler);
    } catch (IOException e) {
      errorHandler.accept(e);
    }
  }

  @Override
  public void stop() {
    binLogStream.stop();
  }
}
