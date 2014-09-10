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
}
