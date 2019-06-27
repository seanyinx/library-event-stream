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

package com.syswin.library.database.event.stream;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterBinlogSyncRecorder implements BinlogSyncRecorder {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final BinlogSyncRecorder recorder;
  private final AtomicLong recordCounter = new AtomicLong();
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

  public CounterBinlogSyncRecorder(BinlogSyncRecorder recorder) {
    this.recorder = recorder;
  }

  @Override
  public void record(String position) {
    recorder.record(position);
    recordCounter.getAndIncrement();
  }

  @Override
  public String position() {
    return recorder.position();
  }

  @Override
  public String recordPath() {
    return recorder.recordPath();
  }

  @Override
  public void flush() {
    recorder.flush();
  }

  @Override
  public void start() {
    recorder.start();
    scheduledExecutor.scheduleWithFixedDelay(
        () -> log.info("Recorded {} binlog events", recordCounter.get()),
        1,
        1,
        MINUTES);
  }

  @Override
  public void shutdown() {
    recorder.shutdown();
    scheduledExecutor.shutdownNow();
  }
}
