package edu.illinois.cs.srg.sim.app;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.cluster.Cluster;
import edu.illinois.cs.srg.sim.job.Job;

import java.util.Map;

/**
 * Created by gourav on 9/11/14.
 */
public class DefaultApplication {

  private String name;
  private Map<Long, Job> jobs;
  Cluster cluster;

  public DefaultApplication(String name) {
    this.name = name;
    jobs = Maps.newHashMap();
    cluster = new Cluster();
  }

  public void add(Job job) {
    jobs.put(job.getId(), job);
  }

  public void schedule(Job job, int index) {


  }

}
