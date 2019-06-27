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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ScheduledStatefulTaskTest {

  private final AtomicInteger deleteCount = new AtomicInteger();
  private final Runnable runnable = Mockito.mock(Runnable.class);

  private final ScheduledStatefulTask scheduledStatefulTask = new ScheduledStatefulTask(runnable, 100L);

  @Before
  public void setUp() {
    doAnswer(invocationOnMock -> deleteCount.getAndIncrement())
        .when(runnable)
        .run();
  }

  @After
  public void tearDown() {
    scheduledStatefulTask.shutdown();
  }

  @Test
  public void deleteEventsPeriodically() {
    scheduledStatefulTask.start(ex -> {});

    waitAtMost(1, SECONDS).untilAsserted(() -> assertThat(deleteCount.get()).isGreaterThanOrEqualTo(1));
  }

  @Test
  public void deleteEventsPeriodicallyOnException() {
    reset(runnable);
    doThrow(RuntimeException.class)
        .doAnswer(invocationOnMock -> deleteCount.getAndIncrement())
        .when(runnable)
        .run();

    scheduledStatefulTask.start(ex -> {});

    waitAtMost(1, SECONDS).untilAsserted(() -> assertThat(deleteCount.get()).isGreaterThanOrEqualTo(1));
  }

  @Test
  public void stopEventHousekeeping() throws InterruptedException {
    scheduledStatefulTask.start(ex -> {});

    waitAtMost(1, SECONDS).untilAsserted(() -> assertThat(deleteCount.get()).isGreaterThanOrEqualTo(1));

    scheduledStatefulTask.stop();
    deleteCount.set(0);
    Thread.sleep(200L);
    assertThat(deleteCount).hasValue(0);
  }
}
