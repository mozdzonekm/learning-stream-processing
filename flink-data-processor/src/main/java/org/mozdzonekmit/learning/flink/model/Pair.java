package org.mozdzonekmit.learning.flink.model;

public class Pair {
  public long low;
  public long high;

  public Pair() {}

  public Pair(long low, long high) {
    this.low = low;
    this.high = high;
  }

  public long getLow() {
    return low;
  }

  public void setLow(long low) {
    this.low = low;
  }

  public long getHigh() {
    return high;
  }

  public void setHigh(long high) {
    this.high = high;
  }

  @Override
  public String toString() {
    return "EventPair{" + "low=" + low + "," + "high" + high + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Pair pair = (Pair) o;
    return pair.low == low && pair.high == high;
  }
}
