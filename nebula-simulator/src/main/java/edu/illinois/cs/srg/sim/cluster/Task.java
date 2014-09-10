package edu.illinois.cs.srg.sim.cluster;

import java.util.Arrays;

/**
 * Created by gourav on 9/8/14.
 */
public class Task {

  private long index;
  private long jobID;
  private int missingInfo;
  private long machineID;
  private JobState state;

  private String user;
  private int schedulingClass;
  private int priority;
  private double cpu;
  private double memory;
  private double diskSpace;
  private int differentMachines;

  public Task(String[] task) {
    if (task.length < 4) {
      throw new RuntimeException("Unknown Google Trace Format: " + Arrays.toString(task));
    }
    this.jobID = Long.parseLong(task[2]);
    this.index = Long.parseLong(task[3]);
    this.state = JobState.UNSUBMITTED;

    this.missingInfo = -1;
    this.machineID = -1;
    this.user = "default";
    this.schedulingClass = -1;
    this.priority = -1;
    this.cpu = -1;
    this.memory = -1;
    this.diskSpace = -1;
    this.differentMachines = 0;
  }

  public long getIndex() {
    return index;
  }

  public long getJobID() {
    return jobID;
  }

  public int getMissingInfo() {
    return missingInfo;
  }

  public long getMachineID() {
    return machineID;
  }

  public JobState getState() {
    return state;
  }

  public String getUser() {
    return user;
  }

  public int getSchedulingClass() {
    return schedulingClass;
  }

  public int getPriority() {
    return priority;
  }

  public double getCpu() {
    return cpu;
  }

  public double getMemory() {
    return memory;
  }

  public double getDiskSpace() {
    return diskSpace;
  }

  public int isDifferentMachines() {
    return differentMachines;
  }

  public void update(String[] taskEvent) {
    if (taskEvent.length > 1 && !taskEvent[1].equals("")) {
      this.missingInfo = Integer.parseInt(taskEvent[1]);
    }
    if (taskEvent.length > 4 && !taskEvent[4].equals("")) {
      this.machineID = Long.parseLong(taskEvent[4]);
    }
    if (taskEvent.length > 5 && !taskEvent[5].equals("")) {
      this.state = JobStateMachine.transition(this.state, Integer.parseInt(taskEvent[5]));
    } else {
      throw new RuntimeException("Missing Task event Type: " +  Arrays.toString(taskEvent));
    }
    if (taskEvent.length > 6 && !taskEvent[6].equals("")) {
      this.user = taskEvent[6];
    }
    if (taskEvent.length > 7 && !taskEvent[7].equals("")) {
      this.schedulingClass = Integer.parseInt(taskEvent[7]);
    }
    if (taskEvent.length > 8 && !taskEvent[8].equals("")) {
      this.priority = Integer.parseInt(taskEvent[8]);
    }
    if (taskEvent.length > 9 && !taskEvent[9].equals("")) {
      this.cpu = Double.parseDouble(taskEvent[9]);
    }
    if (taskEvent.length > 10 && !taskEvent[10].equals("")) {
      this.memory = Double.parseDouble(taskEvent[10]);
    }
    if (taskEvent.length > 11 && !taskEvent[11].equals("")) {
      this.diskSpace = Double.parseDouble(taskEvent[11]);
    }
    if (taskEvent.length > 12 && !taskEvent[12].equals("")) {
      this.differentMachines = Integer.parseInt(taskEvent[12]);
    }
  }


}
