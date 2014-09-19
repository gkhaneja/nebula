package edu.illinois.cs.srg.sim.cluster;

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
  public static long taskEvents = 0;
  public static long constraintEvents = 0;
  public static long machineEvents = 0;
  public static long attributeEvents = 0;
  public static long jobsSubmitted = 0;
  public static long tasksSubmitted = 0;

  public static long constraintsAfterTask = 0;
  public static long constraintsBeforeTask = 0;

  public static long constrainedTasksCount = 0;
  public static long freeTasksCount = 0;

  public static void print() {
    LOG.info("Constraints before tasks: " + Measurements.constraintsBeforeTask);
    LOG.info("Constraints after tasks: " + Measurements.constraintsAfterTask);

    LOG.info("Job events: " + Measurements.jobEvents);
    LOG.info("Task events: " + Measurements.taskEvents);
    LOG.info("Constraints events: " + Measurements.constraintEvents);
    LOG.info("Job submitted: " + Measurements.jobsSubmitted);
    LOG.info("Task submitted: " + Measurements.tasksSubmitted);

    LOG.info("constrainedTasksCount: " + Measurements.constrainedTasksCount);
    LOG.info("freeTasksCount: " + Measurements.freeTasksCount);
  }

}
