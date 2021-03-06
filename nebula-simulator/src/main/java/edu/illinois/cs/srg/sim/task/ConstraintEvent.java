package edu.illinois.cs.srg.sim.task;

import java.util.Arrays;

/**
 * Created by gourav on 9/12/14.
 */
public class ConstraintEvent {

  public static final int EQUAL = 0;
  public static final int NOT_EQUAL = 1;
  public static final int LESS_THAN = 2;
  public static final int GREATER_THAN = 3;

  public static long getTime(String[] event) {
    return Long.parseLong(event[0]);
  }

  public static long getJobID(String[] event) {
    return Long.parseLong(event[1]);
  }

  public static int getIndex(String[] event) {
    return Integer.parseInt(event[2]);
  }

  public static int getOperator(String[] event) {
    return Integer.parseInt(event[3]);
  }

  public static String getName(String[] event) {
    return (event[4]);
  }

  public static String getValue(String[] event) {
    String value;
    try {
      value = event[5];
    } catch (Exception e) {
      value = null;
    }
    return value;
  }
}
