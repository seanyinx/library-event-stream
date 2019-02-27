package com.syswin.library.database.event.stream.integration;

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import java.util.function.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class EventConsumerConfig {

  @Bean
  EventType[] eventTypes() {
    return new EventType[]{TABLE_MAP, EXT_WRITE_ROWS};
  }

  @Bean
  Consumer<Event> eventConsumer() {
    return new MysqlEventHandler("listener_event");
  }
}
