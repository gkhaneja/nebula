package edu.illinois.cs.srg.sim.cluster;

/**
 * Created by gourav on 9/7/14.
 */
public class  MachineEvent {

  public static final int ADD = 0;
  public static final int REMOVE = 1;
  public static final int UPDATE = 2;

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
