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
