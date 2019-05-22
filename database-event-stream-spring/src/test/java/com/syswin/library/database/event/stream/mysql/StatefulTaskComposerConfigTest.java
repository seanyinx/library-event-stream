package com.syswin.library.database.event.stream.mysql;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.syswin.library.database.event.stream.StatefulTaskSupplier;
import com.syswin.library.database.event.stream.integration.TestApplication;
import com.syswin.library.stateful.task.runner.StatefulTask;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestApplication.class})
@ActiveProfiles("dev")
public class StatefulTaskComposerConfigTest {

  private final DataSource dataSource = Mockito.mock(DataSource.class);
  private final StatefulTask statefulTask = Mockito.mock(StatefulTask.class);

  private final Consumer<Throwable> errorHandler = e -> {
  };

  @MockBean
  private StatefulTask suppliedStatefulTask;

  @MockBean
  private StatefulTaskSupplier statefulTaskSupplier;

  @Autowired
  private BiFunction<DataSource, StatefulTask, StatefulTask> statefulTaskComposer;

  @Test
  public void shouldUseStatefulTaskSupplier() {
    when(statefulTaskSupplier.apply(dataSource)).thenReturn(suppliedStatefulTask);

    StatefulTask compositeStatefulTask = statefulTaskComposer.apply(dataSource, statefulTask);

    compositeStatefulTask.start(errorHandler);

    verify(statefulTask).start(errorHandler);
    verify(suppliedStatefulTask).start(errorHandler);
  }
}
