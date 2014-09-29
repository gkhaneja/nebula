package edu.illinois.cs.srg.sim.task;

/**
 * Created by gourav on 9/24/14.
 */
public class TaskDiet {
  long machineID;
  double memory;
  double cpu;

  public TaskDiet(long machineID, double memory, double cpu) {
    this.machineID = machineID;
    this.memory = memory;
    this.cpu = cpu;
  }

  public long getMachineID() {
    return machineID;
  }

  public double getMemory() {
    return memory;
  }

  public double getCpu() {
    return cpu;
  }
}
