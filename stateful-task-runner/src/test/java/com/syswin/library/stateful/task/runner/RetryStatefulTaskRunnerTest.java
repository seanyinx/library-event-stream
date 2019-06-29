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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Test;

public class RetryStatefulTaskRunnerTest {

  private final AtomicBoolean broken = new AtomicBoolean();

  private final AtomicInteger startCounter = new AtomicInteger();
  private final AtomicInteger stopCounter = new AtomicInteger();

  private final StatefulTask underlying = new StatefulTask() {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void start(Consumer<Throwable> errorHandler) {
      startCounter.getAndIncrement();

      if (broken.get()) {
        errorHandler.accept(new RuntimeException("oops"));
      }

      try {
        latch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void stop() {
      stopCounter.getAndIncrement();
      latch.countDown();
    }
  };

  private final RetryStatefulTaskRunner retryStatefulTaskRunner = new RetryStatefulTaskRunner(underlying, 200L);

  @After
  public void tearDown() {
    retryStatefulTaskRunner.shutdown();
  }

  @Test
  public void runUnderlyingTask() {
    retryStatefulTaskRunner.start();

    waitAtMost(1, SECONDS).untilAsserted(() -> assertThat(startCounter).hasValue(1));

    retryStatefulTaskRunner.shutdown();
    assertThat(stopCounter).hasValue(1);
  }

  @Test
  public void retryUnderlyingTask() {
    broken.set(true);
    retryStatefulTaskRunner.start();

    waitAtMost(1, SECONDS).untilAsserted(() -> assertThat(startCounter).hasValueGreaterThan(1));
    assertThat(stopCounter).hasValueGreaterThan(1);
  }
}
