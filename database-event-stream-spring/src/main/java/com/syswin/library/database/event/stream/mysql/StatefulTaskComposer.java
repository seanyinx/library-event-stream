package com.syswin.library.database.event.stream.mysql;

import com.syswin.library.stateful.task.runner.StatefulTask;
import java.util.function.BiFunction;
import javax.sql.DataSource;

public interface StatefulTaskComposer extends BiFunction<DataSource, StatefulTask, StatefulTask> {

}
