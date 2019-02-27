package com.syswin.library.database.event.stream;

public class DbEventStreamException extends RuntimeException {

  public DbEventStreamException(Throwable e) {
    super(e);
  }

  public DbEventStreamException(String message, Throwable e) {
    super(message, e);
  }
}
