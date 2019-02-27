package com.syswin.temail.data.consistency;

import static org.assertj.core.api.Assertions.fail;

import com.syswin.library.stateful.task.runner.StatefulTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
class StatefulTaskConfig {

  @Primary
  @Bean
  StoppableStatefulTask stoppableStatefulTask(StatefulTask task) {
    return new StoppableStatefulTask(task);
  }

  static class StoppableStatefulTask implements StatefulTask {

    private final StatefulTask task;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    StoppableStatefulTask(StatefulTask task) {
      this.task = task;
    }

    @Override
    public void start(Consumer<Throwable> errorHandler) {
      while (paused.get()) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          fail(e.getMessage());
        }
      }

      task.start(errorHandler);
    }

    @Override
    public void stop() {
      task.stop();
    }

    void pause() {
      paused.set(true);
    }

    void resume() {
      paused.set(false);
    }
  }
}
