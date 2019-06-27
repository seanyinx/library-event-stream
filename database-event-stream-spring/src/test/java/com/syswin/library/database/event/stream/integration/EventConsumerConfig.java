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
import com.github.shyiko.mysql.binlog.event.EventType;
import com.syswin.library.database.event.stream.StatefulTaskSupplier;
import com.syswin.library.stateful.task.runner.StatefulTask;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
class EventConsumerConfig {

  private final Queue<String> events = new ConcurrentLinkedQueue<>();

  @Bean
  Queue<String> events() {
    return events;
  }

  @Bean
  EventType[] eventTypes() {
    return new EventType[]{TABLE_MAP, EXT_WRITE_ROWS};
  }

  @Profile("one-source")
  @Bean
  Consumer<Event> eventConsumer() {
    return new MysqlEventHandler(events, "listener_event");
  }

  @Profile("multi-source")
  @Bean
  Function<DataSource, Consumer<Event>> eventConsumerSupplier() {
    return dataSource -> new MysqlEventHandler(events, "listener_event");
  }

  @Profile("dev")
  @Bean
  StatefulTaskSupplier statefulTaskSupplier(StatefulTask statefulTask) {
    return dataSource -> statefulTask;
  }
}
