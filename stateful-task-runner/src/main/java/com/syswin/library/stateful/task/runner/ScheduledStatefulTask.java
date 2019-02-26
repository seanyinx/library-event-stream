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
