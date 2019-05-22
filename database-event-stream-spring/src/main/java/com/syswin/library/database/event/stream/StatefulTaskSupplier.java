package com.syswin.library.database.event.stream;

import com.syswin.library.stateful.task.runner.StatefulTask;
import java.util.function.Function;
import javax.sql.DataSource;

public interface StatefulTaskSupplier extends Function<DataSource, StatefulTask> {

}
