package edu.illinois.cs.srg.sim.cluster;

/**
 * Created by gourav on 9/11/14.
 */
public class EndEvent {

  public static long getTime(String[] event) {
    return Long.parseLong(event[0]);
  }

  public static long getJobID(String[] event) {
    return Long.parseLong(event[1]);
  }

  public static long getIndex(String[] event) {
    return Long.parseLong(event[2]);
  }

  public static long getStartTime(String[] event) {
    return Long.parseLong(event[3]);
  }
}
