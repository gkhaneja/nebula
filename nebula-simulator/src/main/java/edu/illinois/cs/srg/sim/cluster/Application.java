package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by gourav on 9/11/14.
 */
public class Application {

  private String name;
  private Map<Long, Job> jobs;

  public Application(String name) {
    this.name = name;
    jobs = Maps.newHashMap();
  }

  public void add(Job job) {

  }

  public void schedule(Task task) {

  }

}
