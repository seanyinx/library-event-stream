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
