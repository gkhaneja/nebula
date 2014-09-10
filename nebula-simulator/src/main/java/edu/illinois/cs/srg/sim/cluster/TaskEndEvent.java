package edu.illinois.cs.srg.sim.cluster;

import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.IllegalClassFormatException;

/**
 * Created by gourav on 9/9/14.
 */
public class TaskEndEvent implements Comparable<TaskEndEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(TaskEndEvent.class);

  private long jobID;
  private int index;
  private long endTime;
  private long startTime;

  public TaskEndEvent(long jobID, int index, long startTime) {
    this.jobID = jobID;
    this.index = index;
    this.startTime = startTime;
    this.endTime = startTime + Util.getTaskDuration();
  }

  public long getJobID() {
    return jobID;
  }

  public int getIndex() {
    return index;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public int compareTo(TaskEndEvent other) {
    if (other.getEndTime() == this.endTime) {
      return 0;
    } else if (other.getEndTime() < this.endTime) {
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TaskEndEvent)) {
      LOG.error("Cannot compare {} with {}", this, o);
      return false;
    }
    TaskEndEvent other = (TaskEndEvent) o;
    if (other.getEndTime() == this.endTime) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[" + jobID + ", " + index + ", " + startTime + ", " + endTime + "]";
  }
}
