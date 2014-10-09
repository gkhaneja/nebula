package edu.illinois.cs.srg.sim.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gourav on 9/12/14.
 * [main] INFO Simulator - Constraints before tasks: 19560700
 * [main] INFO Simulator - Constraints after tasks:  8924919 - perhaps these were jobs which got respawned ? - not sure.
 *                                                                                               (submitted from DEAD)
 */
public class Measurements {
  private static final Logger LOG = LoggerFactory.getLogger(Measurements.class);

  public static long jobEvents = 0;
  public static long jobSubmitEvents = 0;
  public static long taskEvents = 0;
  public static long taskSubmitEvents = 0;
  public static long machineEvents = 0;
  public static long attributeEvents = 0;
  public static long endEvents = 0;

  public static long jobsSubmitted = 0;
  public static long tasksSubmitted = 0;

  public static long unconsideredTasks = 0;
  public static long unconsideredJobs = 0;

  public static long constraintsAfterTask = 0;
  public static long constraintsBeforeTask = 0;

  public static long constraintEvents = 0;
  public static long constrainedTasksCount = 0;
  public static long freeTasksCount = 0;

  public static long unscheduledTaskCount = 0;
  public static long failedOmegaTransaction = 0;
  public static long totalOmegaTransaction = 0;

  public static long findTime = 0;
  public static long commitTime = 0;
  public static long scheduleTime = 0;



  public static void print() {
    //LOG.info("Constraints before tasks: " + Measurements.constraintsBeforeTask);
    //LOG.info("Constraints after tasks: " + Measurements.constraintsAfterTask + "\n");



    LOG.info("Job events: " + Measurements.jobEvents);
    LOG.info("jobSubmitEvents: " + Measurements.jobSubmitEvents);
    LOG.info("Task events: " + Measurements.taskEvents);
    LOG.info("taskSubmitEvents: " + Measurements.taskSubmitEvents);
    LOG.info("Machine events: " + Measurements.machineEvents);
    LOG.info("Attribute events: " + Measurements.attributeEvents);
    LOG.info("End events: " + Measurements.endEvents + "\n");

    LOG.info("Job submitted: " + Measurements.jobsSubmitted);
    LOG.info("Task submitted: " + Measurements.tasksSubmitted + "\n");

    LOG.info("unconsideredJobs: " + Measurements.unconsideredJobs);
    LOG.info("unconsideredTasks: " + Measurements.unconsideredTasks + "\n");

    LOG.info("Constraints events: " + Measurements.constraintEvents);
    LOG.info("constrainedTasksCount: " + Measurements.constrainedTasksCount);
    LOG.info("freeTasksCount: " + Measurements.freeTasksCount + "\n");

    LOG.info("unscheduledTaskCount: " + Measurements.unscheduledTaskCount);
    LOG.info("failedOmegaTransaction: " + Measurements.failedOmegaTransaction);
    LOG.info("totalOmegaTransaction: " + Measurements.totalOmegaTransaction + "\n");

    LOG.info("findTime: " + Measurements.findTime);
    LOG.info("commitTime: " + Measurements.commitTime);
    LOG.info("scheduleTime: " + Measurements.scheduleTime + "\n");


    LOG.info("----------------------------------------------------------------------------------------------- \n\n");
  }

}
