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

package com.syswin.library.stateful.task.runner;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledStatefulTask implements StatefulTask {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final long scheduledInterval;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final Runnable runnable;
  private volatile boolean started = false;

  public ScheduledStatefulTask(Runnable runnable, long scheduledInterval) {
    this.scheduledInterval = scheduledInterval;
    this.runnable = runnable;
    start();
  }

  @Override
  public void start(Consumer<Throwable> errorHandler) {
    started = true;
  }

  @Override
  public void stop() {
    started = false;
  }

  private void start() {
    scheduledExecutor.scheduleWithFixedDelay(
        () -> {
          if (started) {
            try {
              runnable.run();
            } catch (Exception e) {
              log.error("Failed to run scheduled stateful task", e);
            }
          }
        },
        scheduledInterval, scheduledInterval, MILLISECONDS);
  }

  void shutdown() {
    scheduledExecutor.shutdownNow();
  }
}
