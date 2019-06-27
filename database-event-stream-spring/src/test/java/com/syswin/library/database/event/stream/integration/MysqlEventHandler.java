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

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlEventHandler implements Consumer<Event> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Set<String> tableNames;
  private final Collection<String> events;
  private TableMapEventData eventData;

  MysqlEventHandler(Collection<String> events, String... tableNames) {
    this.tableNames = new HashSet<>();
    Collections.addAll(this.tableNames, tableNames);
    this.events = events;
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

  Collection<String> events() {
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
