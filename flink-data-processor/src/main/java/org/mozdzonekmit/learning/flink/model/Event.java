package org.mozdzonekmit.learning.flink.model;

public class Event {
  public long num;

  public Event() {}

  public Event(long num) {
    this.num = num;
  }

  public Long getPairKey(long pairSum) {
    long complementaryNum = pairSum - num;
    if (complementaryNum < num) {
      return complementaryNum;
    }
    return num;
  }

  public long getNum() {
    return num;
  }

  public void setNum(long num) {
    this.num = num;
  }

  @Override
  public String toString() {
    return "Event{" + "num=" + num + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Event event = (Event) o;
    return event.num == num;
  }
}
