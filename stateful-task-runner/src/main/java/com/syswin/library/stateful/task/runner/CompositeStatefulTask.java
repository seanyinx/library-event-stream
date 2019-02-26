package com.syswin.library.stateful.task.runner;

import java.util.function.Consumer;

public class CompositeStatefulTask implements StatefulTask {

  private final StatefulTask[] tasks;

  /**
   * {@link StatefulTask#start(Consumer)} of the last argument <b>must</b> be blocking until task finished
   * or interrupted by exception, because it is called asynchronously.
   * Non-blocking implementation will be executed multiple times concurrently, causing unexpected behavior.
   */
  public CompositeStatefulTask(StatefulTask... tasks) {
    this.tasks = tasks;
  }

  @Override
  public void start(Consumer<Throwable> errorHandler) {
    for (StatefulTask task : tasks) {
      task.start(errorHandler);
    }
  }

  @Override
  public void stop() {
    for (StatefulTask task : tasks) {
      task.stop();
    }
  }
}
