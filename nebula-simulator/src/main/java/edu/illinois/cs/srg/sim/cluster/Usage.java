package edu.illinois.cs.srg.sim.cluster;

/**
 * Created by gourav on 9/25/14.
 */
public class Usage {
  public double memory;
  public double cpu;

  public Usage() {
    memory = 0;
    cpu = 0;
  }

  public Usage(Usage usage) {
    memory = usage.memory;
    cpu = usage.cpu;
  }

  @Override
  public String toString() {
    return "(" + memory + "," + cpu + ")";
  }
}
