package edu.illinois.cs.srg.sim.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by gourav on 9/10/14.
 */
public class TaskEvent {
  private static final Logger LOG = LoggerFactory.getLogger(TaskEvent.class);

  public static final int SIZE = 13;

  public static long getTimestamp(String[] event) {
    return Long.parseLong(event[0]);
  }

  public static long getJobID(String[] event) {
    return Long.parseLong(event[2]);
  }

  public static int getIndex(String[] event) {
    return Integer.parseInt(event[3]);
  }

  public static long getEventType(String[] event) {
    return Long.parseLong(event[5]);
  }

  public static double getCPU(String[] event) {
    double cpu;
    try {
      cpu = Double.parseDouble(event[9]);
    } catch (Exception e) {
      cpu = 0;
    }
    return cpu;
  }

  public static double getMemory(String[] event) {
    double memory;
    try {
      memory = Double.parseDouble(event[10]);
    } catch (Exception e) {
      memory = 0;
    }
    return memory;
  }


}
