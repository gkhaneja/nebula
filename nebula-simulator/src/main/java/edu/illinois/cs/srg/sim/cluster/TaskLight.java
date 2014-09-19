package edu.illinois.cs.srg.sim.cluster;

/**
 * Created by gourav on 9/14/14.
 */
public class TaskLight {
  private int index;
  private long jobID;

  public TaskLight(long jobID, int index) {
    this.index = index;
    this.jobID = jobID;
  }

  public TaskLight(String taskAsString) {
    String[] parts = taskAsString.trim().split("-");
    this.jobID = Long.parseLong(parts[0]);
    this.index = Integer.parseInt(parts[1]);
  }

  public int getIndex() {
    return index;
  }

  public long getJobID() {
    return jobID;
  }

  @Override
  public int hashCode() {
    long sum = jobID + index;
    while (sum > Integer.MAX_VALUE) {
      sum /= 351;
    }
    int hashCode = (int) sum;
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TaskLight)) {
      return false;
    }
    TaskLight other = (TaskLight) o;
    if (this.jobID == other.getJobID() && this.index == other.getIndex()) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return jobID + "-" + index;
  }
}
