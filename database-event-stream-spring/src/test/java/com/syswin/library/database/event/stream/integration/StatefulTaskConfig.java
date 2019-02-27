package com.syswin.library.database.event.stream.integration;

import static org.assertj.core.api.Assertions.fail;

import com.syswin.library.stateful.task.runner.StatefulTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

@Component
class StatefulTaskConfig implements BeanPostProcessor {

  @Override
  public Object postProcessBeforeInitialization(@NotNull Object bean, String beanName) throws BeansException {
    if (bean instanceof StatefulTask) {
      return new StoppableStatefulTask((StatefulTask) bean);
    }
    return bean;
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
