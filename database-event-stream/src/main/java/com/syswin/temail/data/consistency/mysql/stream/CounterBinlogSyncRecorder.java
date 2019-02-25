package com.syswin.temail.data.consistency.mysql.stream;

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
