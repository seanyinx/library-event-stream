package com.syswin.temail.data.consistency.mysql.stream;

import java.util.function.Consumer;

public interface StatefulTask {

  /**
   * The task to run when acquired leadership. The implementation of this method <b>must</b> be blocking
   * until task finished or interrupted by exception, because it is called asynchronously. Non-blocking
   * implementation of this method will be executed multiple times concurrently, causing unexpected behavior.
   * @param errorHandler the error handler to handle exception which interrupted the task. This handler
   * will call {@link StatefulTask#stop()} to suspend task execution.
   */
  void start(Consumer<Throwable> errorHandler);

  void stop();
}
