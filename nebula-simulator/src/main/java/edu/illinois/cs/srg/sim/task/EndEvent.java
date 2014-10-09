package edu.illinois.cs.srg.sim.task;

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

  public static int getIndex(String[] event) {
    return Integer.parseInt(event[2]);
  }

  public static long getStartTime(String[] event) {
    return Long.parseLong(event[3]);
  }

  public static double getCPU(String[] event) {
    double cpu;
    try {
      cpu = Double.parseDouble(event[4]);
    } catch (Exception e) {
      cpu = 0;
    }
    return cpu;
  }

  public static double getMemory(String[] event) {
    double memory;
    try {
      memory = Double.parseDouble(event[5]);
    } catch (Exception e) {
      memory = 0;
    }
    return memory;
  }

}
