package edu.illinois.cs.srg.sim.util;

import edu.illinois.cs.srg.sim.cluster.Usage;

/**
 * Created by gourav on 9/25/14.
 */
public class UsageUtil {

  public static void add(Usage usage, double memory, double cpu) {
    if (memory < 0 || cpu < 0) {
      throw new RuntimeException("Memory and CPU should be non-negative: " + memory + ", " + cpu);
    }
    usage.memory += memory;
    usage.cpu += cpu;
  }

  public static boolean check(Usage usage, double memory, double cpu, double maxMemory, double maxCpu) {
    if (memory < 0 || cpu < 0) {
      throw new RuntimeException("Memory and CPU should be non-negative: " + memory + ", " + cpu);
    }
    if (memory + usage.memory <= maxMemory && cpu + usage.cpu <= maxCpu) {
      return true;
    }
    return false;
  }
}
