package edu.illinois.cs.srg.sim.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gourav on 9/16/14.
 */
public class TimeTracker {
  private static Logger LOG = LoggerFactory.getLogger(TimeTracker.class);

  private long startTime;
  private String name;

  public TimeTracker(String name) {
    this.name = name;
    this.startTime = System.currentTimeMillis();
  }

  public TimeTracker(String name, long startTime) {
    this.name = name;
    this.startTime = startTime;
  }

  public void checkpoint(String message) {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }
    LOG.info(name + message);
    long time = (System.currentTimeMillis() - startTime);
    if (time > 1000) {
      LOG.info("Time Taken: {} seconds", time / 1000);
    } else {
      LOG.info("Time Taken: {} milliseconds", time);

    }

    LOG.info("Memory:" + Util.memoryMXBean.getHeapMemoryUsage().toString() + "\n");
    startTime = System.currentTimeMillis();
  }

  public void checkpoint() {
    checkpoint("");
  }
}
