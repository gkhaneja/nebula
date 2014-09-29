package edu.illinois.cs.srg.sim.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by gourav on 9/9/14.
 */
public class Event implements Comparable<Event> {
  private static final Logger LOG = LoggerFactory.getLogger(Event.class);


  private long time;
  private String[] event;

  public Event(long time) {
    this.time = time;
    this.event = new String[0];
  }

  public Event(String[] event) {
    try {
      this.time = Long.parseLong(event[0]);
    } catch (NumberFormatException e) {
      // Inconsistency in trace usage of 2^64 - 1 instead of 2^63 - 1 to represent events at the end.
      LOG.warn("Replacing timestamp from {} to {} in {}", event[0], Long.MAX_VALUE, event);
      this.time = Long.MAX_VALUE;
    }
    this.event = event;
  }

  public Event(long time, String[] event) {
    this.time = time;
    this.event = event;
  }

  public long getTime() {
    return time;
  }

  public String[] getEvent() {
    return event;
  }

  @Override
  public int compareTo(Event other) {
    if (other.getTime() == this.time) {
      return 0;
    } else if (other.getTime() < this.time) {
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Event)) {
      LOG.error("Cannot compare {} with {}", this, o);
      return false;
    }
    Event other = (Event) o;
    if (other.getTime() == this.time) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "-" + Arrays.toString(event);
  }
}
