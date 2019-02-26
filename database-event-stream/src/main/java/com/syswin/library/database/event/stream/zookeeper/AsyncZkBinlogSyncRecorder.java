package com.syswin.library.database.event.stream.zookeeper;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncZkBinlogSyncRecorder extends ZkBinlogSyncRecorder {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final long updateIntervalMillis;
  private final AtomicBoolean updated = new AtomicBoolean();
  private volatile String position;

  public AsyncZkBinlogSyncRecorder(String clusterName, CuratorFramework curator, long updateIntervalMillis) {
    super(clusterName, curator);
    this.updateIntervalMillis = updateIntervalMillis;
  }

  @Override
  public void record(String position) {
    this.position = position;
    updated.set(true);
    log.trace("Saved binlog position [{}] locally", position);
  }

  @Override
  public void flush() {
    flushIfUpdated();
    super.flush();
  }

  @Override
  public void start() {
    super.start();
    scheduledExecutor.scheduleWithFixedDelay(this::flushIfUpdated, updateIntervalMillis, updateIntervalMillis, MILLISECONDS);
  }

  private void flushIfUpdated() {
    if (updated.compareAndSet(true, false)) {
      updatePositionToZk(position);
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    scheduledExecutor.shutdownNow();
  }
}
