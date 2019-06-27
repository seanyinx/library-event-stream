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

  public AsyncZkBinlogSyncRecorder(String rootPath, String clusterName, CuratorFramework curator, long updateIntervalMillis) {
    super(rootPath, clusterName, curator);
    this.updateIntervalMillis = updateIntervalMillis;
  }

  @Override
  public void record(String position) {
    this.position = position;
    updated.set(true);
    log.trace("Saved binlog position [{}] locally", position);
  }

  @Override
  public void start() {
    super.start();
    scheduledExecutor.scheduleWithFixedDelay(this::flushIfUpdated, updateIntervalMillis, updateIntervalMillis, MILLISECONDS);
  }

  @Override
  void flushIfUpdated() {
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
