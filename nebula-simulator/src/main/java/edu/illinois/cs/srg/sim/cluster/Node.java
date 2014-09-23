package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gourav on 9/3/14.
 */
public class Node {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  private long id;
  private double cpu;
  private double memory;
  private String platformID;
  private Map<String, String> attributes;
  private boolean isDeleted;

  private Usage usage;

  public Node(String[] googleTrace) {
    if (googleTrace.length < 3) {
      throw new RuntimeException("Unknown Google Trace Format: " + Arrays.toString(googleTrace));
    }
    this.id = Long.parseLong(googleTrace[1]);
    this.platformID = "";
    this.cpu = 0;
    this.memory = 0;
    this.isDeleted = false;
    update(googleTrace);
    attributes = Maps.newHashMap();
    usage = new Usage();
  }

  public long getId() {
    return id;
  }

  public double getCpu() {
    return cpu;
  }

  public double getMemory() {
    return memory;
  }

  public String getPlatformID() {
    return platformID;
  }

  public void addAttribute(String name, String value) {
    attributes.put(name, value);
  }

  // Remains silent if the attribute is non-existent.
  public void removeAttribute(String name) {
    if (!attributes.containsKey(name)) {
      // Only 30 such inconsistencies are observed. Ignoring them for now.
      //LOG.warn("Cannot remove non-existent attribute: {}, {}", id, name);
    } else {
      attributes.remove(name);
    }
  }

  public String getAttribute(String name) {
    return attributes.get(name);
  }

  public void markDeleted(String[] googleTrace) {
    this.isDeleted = true;
    update(googleTrace);
  }

  public void unmarkDeleted(String[] googleTrace) {
    this.isDeleted = false;
    update(googleTrace);
  }

  public boolean isDeleted() {
    return this.isDeleted;
  }

  public void update(String[] googleTrace) {
    if (googleTrace.length > 3 && !googleTrace[3].equals("")) {
      this.platformID = googleTrace[3];
    }
    if (googleTrace.length > 4 && !googleTrace[4].equals("")) {
      this.cpu = Double.parseDouble(googleTrace[4]);
    }
    if (googleTrace.length > 5 && !googleTrace[5].equals("")) {
      this.memory = Double.parseDouble(googleTrace[5]);
    }
  }

  public Usage getUsage() {
    return usage;
  }

  public class Usage {
    // Currently, only store the total cpu and memory being used.
    // TODO: Store per app / job memory , cpu usage.
    // TODO: Reserve some resources for scheduler and OS.

    private double memoryUsed;
    private double cpuUsed;

    public Usage() {
      memoryUsed = 0;
      cpuUsed = 0;
    }

    /**
     * Returns false if operation cannot be performed.
     * Returns true, otherwise.
     * @param memory
     * @param cpu
     * @return
     */
    public boolean add(double memory, double cpu) {
      if (memory < 0 || cpu < 0) {
        throw new RuntimeException("Memory and CPU should be non-negative: " + memory + ", " + cpu);
      }
      if (memory + memoryUsed <= Node.this.memory && cpu + cpuUsed <= Node.this.cpu) {
        memoryUsed += memory;
        cpuUsed += cpu;
        return true;
      }
      return false;
    }

    public boolean check(double memory, double cpu) {
      if (memory < 0 || cpu < 0) {
        throw new RuntimeException("Memory and CPU should be non-negative: " + memory + ", " + cpu);
      }
      if (memory + memoryUsed <= Node.this.memory && cpu + cpuUsed <= Node.this.cpu) {
        return true;
      }
      return false;
    }

    public void release(double memory, double cpu) {
      if (memory < 0 || cpu < 0) {
        throw new RuntimeException("Memory and CPU should be non-negative: " + memory + ", " + cpu);
      }
      memoryUsed = memoryUsed - memory;
      cpuUsed = cpuUsed - cpu;
    }

    public void release(Resource resource) {
      release(resource.getMemory(), resource.getCpu());
    }

    public double getMemory() {
      return memoryUsed;
    }

    public double getCpu() {
      return cpuUsed;
    }

  }

  public static class Resource {
    private double memory;
    private double cpu;

    public Resource(double memory, double cpu) {
      this.memory = memory;
      this.cpu = cpu;
    }

    public double getMemory() {
      return memory;
    }

    public double getCpu() {
      return cpu;
    }
  }
}
