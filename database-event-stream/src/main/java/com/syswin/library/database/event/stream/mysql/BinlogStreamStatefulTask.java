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
