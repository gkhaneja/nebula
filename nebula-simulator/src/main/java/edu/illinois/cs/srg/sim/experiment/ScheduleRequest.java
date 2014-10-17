package edu.illinois.cs.srg.sim.experiment;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by gourav on 10/17/14.
 */
public class ScheduleRequest implements Serializable {

  // Job id
  long id;
  Set<Task> tasks;

  public ScheduleRequest(long id, Set<Task> tasks) {
    this.id = id;
    this.tasks = tasks;
  }

  static class Task implements Serializable {
    int index;
    double cpu;
    double memory;

    Task(int index, double cpu, double memory) {
      this.index = index;
      this.cpu = cpu;
      this.memory = memory;
    }
  }
}
