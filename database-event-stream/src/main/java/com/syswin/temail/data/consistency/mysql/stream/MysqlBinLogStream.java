package com.syswin.temail.data.consistency.mysql.stream;

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlBinLogStream {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final BinaryLogClient client;
  private final String hostname;
  private final int port;
  private final long serverId;
  private final BinlogSyncRecorder binlogSyncRecorder;

  public MysqlBinLogStream(String hostname,
      int port,
      String username,
      String password,
      long serverId,
      BinlogSyncRecorder binlogSyncRecorder) {

    this.hostname = hostname;
    this.port = port;
    this.serverId = serverId;
    this.binlogSyncRecorder = binlogSyncRecorder;
    this.client = new BinaryLogClient(hostname, port, username, password);
  }

  void start(Consumer<Event> eventHandler, Consumer<Throwable> errorHandler, EventType... eventTypes) throws IOException {
    client.setServerId(serverId);
    client.setGtidSetFallbackToPurged(true);
    client.setGtidSet(binlogSyncRecorder.position());
    client.setEventDeserializer(createEventDeserializerOf(eventTypes));
    client.registerEventListener(replicationEventListener(eventHandler, errorHandler));
    client.registerLifecycleListener(new MySqlLifecycleListener(hostname, port, binlogSyncRecorder, errorHandler));

    log.info("Connecting to Mysql at {}:{} from binlog [{}]", hostname, port, client.getGtidSet());
    client.connect();
  }

  void stop() {
    try {
      client.disconnect();
      client.getEventListeners().forEach(client::unregisterEventListener);
      client.getLifecycleListeners().forEach(client::unregisterLifecycleListener);
      binlogSyncRecorder.flush();
      log.info("Disconnected from Mysql at {}:{}", hostname, port);
    } catch (IOException e) {
      log.warn("Failed to disconnect from MySql at {}:{}", hostname, port, e);
    }
  }

  private BinaryLogClient.EventListener replicationEventListener(
      Consumer<Event> eventHandler,
      Consumer<Throwable> errorHandler) {
    return new TableEventListener(eventHandler, errorHandler);
  }

  private EventDeserializer createEventDeserializerOf(EventType... includedTypes) {
    EventDeserializer eventDeserializer = new EventDeserializer();

    eventDeserializer.setCompatibilityMode(
        DATE_AND_TIME_AS_LONG,
        CHAR_AND_BINARY_AS_BYTE_ARRAY
    );

    EventDataDeserializer nullEventDataDeserializer = new NullEventDataDeserializer();

    Set<EventType> includedEventTypes = new HashSet<>();
    Collections.addAll(includedEventTypes, includedTypes);
    log.debug("Only interested events will be serialized: {}", includedEventTypes);

    for (EventType eventType : EventType.values()) {
      if (!includedEventTypes.contains(eventType)) {
        eventDeserializer.setEventDataDeserializer(eventType, nullEventDataDeserializer);
      }
    }

    return eventDeserializer;
  }

  private class TableEventListener implements EventListener {

    private final Consumer<Event> eventConsumer;
    private final Consumer<Throwable> errorHandler;

    TableEventListener(Consumer<Event> eventConsumer, Consumer<Throwable> errorHandler) {
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }

    @Override
    public void onEvent(Event event) {
      log.trace("Received binlog event {}", event);
      if (event.getData() != null) {
        handleDeserializedEvent(event);
      }
      if (!client.getGtidSet().isEmpty()) {
        binlogSyncRecorder.record(latestGTID());
      }
    }

    private void handleDeserializedEvent(Event event) {
      try {
        eventConsumer.accept(event);
      } catch (Exception e) {
        errorHandler.accept(e);
        throw e;
      }
    }

    // the known GTID set format: master server UUID:sequence_no_range
    // e.g. 3809c41e-34fb-11e9-a425-0242ac140002:1-4
    // the last seen GTID is therefore 3809c41e-34fb-11e9-a425-0242ac140002:4
    private String latestGTID() {
      return client.getGtidSet();
    }
  }

  private static class MySqlLifecycleListener implements LifecycleListener {

    private final String hostname;
    private final int port;
    private final BinlogSyncRecorder binlogSyncRecorder;
    private final Consumer<Throwable> errorHandler;

    MySqlLifecycleListener(String hostname,
        int port,
        BinlogSyncRecorder binlogSyncRecorder,
        Consumer<Throwable> errorHandler) {

      this.hostname = hostname;
      this.port = port;
      this.binlogSyncRecorder = binlogSyncRecorder;
      this.errorHandler = errorHandler;
    }

    @Override
    public void onConnect(BinaryLogClient client) {
      log.info("Connected to Mysql at {}:{} on server {} starting from binlog position [{}]",
          hostname,
          port,
          client.getServerId(),
          binlogSyncRecorder.position());
    }

    @Override
    public void onDisconnect(BinaryLogClient client) {
      log.info("Disconnected from Mysql at {}:{} on server {} and current binlog position is [{}]",
          hostname,
          port,
          client.getServerId(),
          binlogSyncRecorder.position());
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
      logError(client, ex, "Communication failure with");
      errorHandler.accept(ex);
    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
      logError(client, ex, "Failed to deserialize event from");
    }

    private void logError(BinaryLogClient client, Exception ex, String description) {
      log.error("{} Mysql at {}:{} on server {} and current binlog position is [{}]",
          description,
          hostname,
          port,
          client.getServerId(),
          binlogSyncRecorder.position(),
          ex);
    }
  }
}
