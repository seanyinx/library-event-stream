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
