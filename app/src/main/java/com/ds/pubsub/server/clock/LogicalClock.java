package com.ds.pubsub.server.clock;

import java.util.concurrent.atomic.AtomicReference;

public class LogicalClock {

  private final AtomicReference<LogicalTimestamp> timeReference = new AtomicReference<>();

  /** Creates instance of logical clock with default initial timestamp. */
  public LogicalClock() {
    this(new LogicalTimestamp());
  }

  /** Creates instance of logical clock with the given initial timestamp. */
  public LogicalClock(LogicalTimestamp initialTimestamp) {
    this.timeReference.set(initialTimestamp);
  }

  /** Returns current value of the clock. */
  public LogicalTimestamp time() {
    return timeReference.get();
  }

  /**
   * Increments the clock time and returns newly set value of the clock.
   *
   * @return New value of the clock.
   */
  public LogicalTimestamp tick() {
    LogicalTimestamp previousTimestamp, nextTimestamp;
    do {
      previousTimestamp = timeReference.get();
      nextTimestamp = previousTimestamp.nextTimestamp();
    } while (!timeReference.compareAndSet(previousTimestamp, nextTimestamp));
    return nextTimestamp;
  }

  /**
   * Increments the value of the clock taking into account that provided timestamp happens before
   * that moment. Returns new value of the clock which happens after previous value of the clock and
   * provided timestamp.
   *
   * @param happensBeforeTimestamp timestamp value which happens in the past
   * @return New value of the clock.
   */
  public LogicalTimestamp tick(LogicalTimestamp happensBeforeTimestamp) {
    LogicalTimestamp previousTimestamp, nextTimestamp;
    do {
      previousTimestamp = timeReference.get();
      if (previousTimestamp.isAfter(happensBeforeTimestamp)) {
        nextTimestamp = previousTimestamp.nextTimestamp();
      } else {
        nextTimestamp = happensBeforeTimestamp.nextTimestamp();
      }
    } while (!timeReference.compareAndSet(previousTimestamp, nextTimestamp));
    return nextTimestamp;
  }
}
