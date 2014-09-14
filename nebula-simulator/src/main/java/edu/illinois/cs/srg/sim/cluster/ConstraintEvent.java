package edu.illinois.cs.srg.sim.cluster;

/**
 * Created by gourav on 9/12/14.
 */
public class ConstraintEvent {
  public static long getTime(String[] event) {
    return Long.parseLong(event[0]);
  }

  public static long getJobID(String[] event) {
    return Long.parseLong(event[1]);
  }

  public static long getIndex(String[] event) {
    return Long.parseLong(event[2]);
  }

  public static int getOperator(String[] event) {
    return Integer.parseInt(event[3]);
  }

  public static String getName(String[] event) {
    return (event[4]);
  }

  public static String getValue(String[] event) {
    return (event[5]);
  }
}
