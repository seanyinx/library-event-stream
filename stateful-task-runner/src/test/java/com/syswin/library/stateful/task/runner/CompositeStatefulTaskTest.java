package com.syswin.library.stateful.task.runner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.Mockito;

public class CompositeStatefulTaskTest {

  private final List<StatefulTask> tasks = new ArrayList<>(2);
  private final Consumer<Throwable> errorHandler = ex -> {
  };
  private final StatefulTask task1 = Mockito.mock(StatefulTask.class);
  private final StatefulTask task2 = Mockito.mock(StatefulTask.class);

  private final CompositeStatefulTask compositeStatefulTask = new CompositeStatefulTask(task1, task2);

  @Test
  public void startsTaskInOrder() {
    doAnswer(invocationOnMock -> tasks.add(task1)).when(task1).start(errorHandler);
    doAnswer(invocationOnMock -> tasks.add(task2)).when(task2).start(errorHandler);

    compositeStatefulTask.start(errorHandler);

    assertThat(tasks).containsExactly(task1, task2);
  }

  @Test
  public void stopsTaskInOrder() {
    doAnswer(invocationOnMock -> tasks.add(task1)).when(task1).stop();
    doAnswer(invocationOnMock -> tasks.add(task2)).when(task2).stop();

    compositeStatefulTask.stop();

    assertThat(tasks).containsExactly(task1, task2);
  }
}
