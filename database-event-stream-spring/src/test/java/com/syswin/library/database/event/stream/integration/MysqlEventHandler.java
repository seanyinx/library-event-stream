package com.syswin.library.database.event.stream.integration;

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlEventHandler implements Consumer<Event> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Set<String> tableNames;
  private final ArrayList<String> events = new ArrayList<>();
  private TableMapEventData eventData;

  MysqlEventHandler(String... tableNames) {
    this.tableNames = new HashSet<>();
    Collections.addAll(this.tableNames, tableNames);
  }

  @Override
  public void accept(Event event) {
    if (TABLE_MAP.equals(event.getHeader().getEventType())) {
      TableMapEventData data = event.getData();
      if (tableNames.contains(data.getTable())) {
        log.debug("Processing binlog event: {}", event);
        eventData = data;
      }
    } else if (EXT_WRITE_ROWS.equals(event.getHeader().getEventType()) && eventData != null) {
      log.debug("Processing binlog event: {}", event);
      handleInsertEvent(event);
    }
  }

  List<String> events() {
    return events;
  }

  private void handleInsertEvent(Event event) {
    WriteRowsEventData data = event.getData();

    if (data.getTableId() == eventData.getTableId()) {
      data.getRows().forEach(columns ->
          events.add(new String((byte[]) columns[2]) + "," +
              new String((byte[]) columns[3]) + "," +
              new String((byte[]) columns[4])));
    }

    eventData = null;
  }
}
