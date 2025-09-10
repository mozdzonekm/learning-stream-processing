package org.mozdzonekmit.learning.flink.model;

public class DeserializationResult<T> {
  private final T event;
  private final byte[] raw;
  private final Exception error;

  public DeserializationResult(T event, byte[] raw, Exception error) {
    this.event = event;
    this.raw = raw;
    this.error = error;
  }

  public boolean isSuccess() {
    return error == null;
  }

  public T getEvent() {
    return event;
  }

  public byte[] getRaw() {
    return raw;
  }

  public Exception getError() {
    return error;
  }
}
