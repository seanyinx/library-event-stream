package com.syswin.temail.data.consistency.mysql.stream;

public interface BinlogSyncRecorder {

  void record(String position);

  String position();

  String recordPath();

  void flush();

  default void start() {
  }

  default void shutdown() {
  }
}
