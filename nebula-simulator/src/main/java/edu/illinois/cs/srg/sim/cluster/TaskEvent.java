package edu.illinois.cs.srg.sim.cluster;

/**
 * Created by gourav on 9/10/14.
 */
public class TaskEvent {


  public static long getTimestamp(String[] event) {
    return Long.parseLong(event[0]);
  }

  public static long getJobID(String[] event) {
    return Long.parseLong(event[2]);
  }

  public static long getIndex(String[] event) {
    return Long.parseLong(event[3]);
  }

  public static long getEventType(String[] event) {
    return Long.parseLong(event[5]);
  }


}
