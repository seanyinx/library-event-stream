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
