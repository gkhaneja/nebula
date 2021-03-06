package edu.illinois.cs.srg.sim.omega;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import edu.illinois.cs.srg.sim.app.RoundRobinAppFilter;
import edu.illinois.cs.srg.sim.cluster.*;
import edu.illinois.cs.srg.sim.job.JobEvent;
import edu.illinois.cs.srg.sim.task.ConstraintEvent;
import edu.illinois.cs.srg.sim.task.EndEvent;
import edu.illinois.cs.srg.sim.task.TaskDiet;
import edu.illinois.cs.srg.sim.task.TaskEvent;
import edu.illinois.cs.srg.sim.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by gourav on 9/9/14.
 */
public class OmegaSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(OmegaSimulator.class);
  static final boolean DEBUG = false;

  public static Cluster cluster;
  private static OmegaScheduler scheduler;
  private static Map<String, OmegaApplication> applications;

  // Helping variable.
  private static Iterator<String[]> constraintIterator;
  private static String[] lastConstraint;
  private static Queue<Event> taskEndEvents;
  private static Map<Long, String> jobs;
  private static RoundRobinAppFilter appFilter;

  private static TimeTracker timeTracker = new TimeTracker("Global OmegaSimulator: ");

  public static void main(String[] args) {

    //NebulaConfiguration.init(OmegaSimulator.class.getResourceAsStream(Constants.NEBULA_SITE));
    cluster = new Cluster();
    scheduler = new OmegaScheduler();
    applications = Maps.newHashMap();

    taskEndEvents = Queues.newPriorityQueue();
    jobs = Maps.newHashMap();
    appFilter = new RoundRobinAppFilter(1500);

    TimeTracker timeTracker2 = new TimeTracker("OmegaSimulator: ");
    try {
      OmegaSimulator.simulate();
    } catch (Exception e) {
      timeTracker2.checkpoint("Simulation failed.");
      //TODO: save the state
      throw new RuntimeException(e);
    }
    timeTracker2.checkpoint("Finished Simulation.");
  }

  public static void simulate() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader("/Users/gourav/projects/googleTraceData/clusterdata-2011-1");

    String pattern =  "part-0000[1-9]-of-[0-9]*.csv";

    Iterator<String[]> jobIterator;
    Iterator<String[]> taskIterator;
    Iterator<String[]> attributeIterator;
    Iterator<String[]> machineIterator;

    if (DEBUG) {
      jobIterator = googleTraceReader.open(Constants.DEBUG_SUBMIT_JOB_EVENTS);
      taskIterator = googleTraceReader.open(Constants.DEBUG_SUBMIT_TASK_EVENTS, pattern);
      constraintIterator = googleTraceReader.open(Constants.DEBUG_FILTERED_CONSTRAINTS, pattern);
      attributeIterator = googleTraceReader.open(Constants.DEBUG_MACHINE_ATTRIBUTES);
      machineIterator = googleTraceReader.open(Constants.DEBUG_MACHINE_EVENTS);
    } else {
      jobIterator = googleTraceReader.open(Constants.SUBMIT_JOB_EVENTS);
      taskIterator = googleTraceReader.open(Constants.SUBMIT_TASK_EVENTS, pattern);
      constraintIterator = googleTraceReader.open(Constants.FILTERED_CONSTRAINED, pattern);
      attributeIterator = googleTraceReader.open(Constants.MACHINE_ATTRIBUTES);
      machineIterator = googleTraceReader.open(Constants.MACHINE_EVENTS);
    }

    Event job = null;
    if (jobIterator.hasNext()) {
      job = new Event(jobIterator.next());
    }

    Event task = null;
    if (taskIterator.hasNext()) {
      task = new Event(taskIterator.next());
    }

    Event machine = null;
    if (machineIterator.hasNext()) {
      machine = new Event(machineIterator.next());
    }

    Event attribute = null;
    if (attributeIterator.hasNext()) {
      attribute = new Event(attributeIterator.next());
    }

    Event end = taskEndEvents.peek();

    long startTime = System.currentTimeMillis();

    while (keepRolling(machine, attribute, job, task, end)) {

      if (System.currentTimeMillis() - startTime > 100000 && task != null) {
        startTime = System.currentTimeMillis();
        LOG.info("Current Simulation Time: " + TaskEvent.getTimestamp(task.getEvent()));
        Measurements.print();
        timeTracker.checkpoint();
      }

      switch (next(machine, attribute, job, task, end)) {
        case 0:
          // process machine event
          Measurements.machineEvents++;
          processMachineEvent(machine.getEvent());
          machine = null;
          if (machineIterator.hasNext()) {
            machine = new Event(machineIterator.next());
          }
          break;
        case 1:
          // process attribute event
          Measurements.attributeEvents++;
          processMachineAttribute(attribute.getEvent());
          attribute = null;
          if (attributeIterator.hasNext()) {
            attribute = new Event(attributeIterator.next());
          }
          break;
        case 2:
          // process job event
          Measurements.jobEvents++;
          processJobEvent(job);
          job = null;
          if (jobIterator.hasNext()) {
            job = new Event(jobIterator.next());
          }
          break;
        case 3:
          // process task event
          processSubmitTaskEvent(task);
          Measurements.taskEvents++;
          task = null;
          if (taskIterator.hasNext()) {
            task = new Event(taskIterator.next());
          }
          break;
        case 4:
          // process end event
          Measurements.endEvents++;
          processEndEvent(end);
          taskEndEvents.poll();
          break;
        default:
          LOG.warn("Unknown event.");
      }
      end = taskEndEvents.peek();
    }

    // Validation: all constraints are accounted for.
    if (constraintIterator.hasNext()) {
      LOG.error("Inconsistency: There are constraints left to read.");
    }

    Measurements.print();
    LOG.info("No. of total nodes added: " + cluster.getSize());
  }

  private static void processJobEvent(Event event) {
    LOG.debug("Process Job Event {}", event);
    if (event == null) {
      return;
    }
    // Only processing SUBMIT events from traces. Other events should come from scheduler.

    Measurements.jobSubmitEvents++;
    String app = appFilter.getApp(event.getEvent());
    if (app != null) {
      Measurements.jobsSubmitted++;
      if (app.equals("")) {
        app = Constants.DEFAULT_JOB_NAME;
      }
      jobs.put(JobEvent.getID(event), app);
      if (!applications.containsKey(app)) {
        applications.put(app, new OmegaApplication(app, scheduler));
      }
    } else {
      Measurements.unconsideredJobs++;
    }
  }


  private static void processSubmitTaskEvent(Event event) {
    LOG.debug("Process Task Event {}", event);
    if (event == null) {
      return;
    }
    long jobID = TaskEvent.getJobID(event.getEvent());
    int index = TaskEvent.getIndex(event.getEvent());

    Measurements.taskSubmitEvents++;
    String app = jobs.get(jobID);
    if (app != null) {
      Measurements.tasksSubmitted++;

      // 1. Get all constraints associated with the task.
      List<String[]> currentConstraints = Lists.newArrayList();
      if (lastConstraint != null) {
        if (jobID == ConstraintEvent.getJobID(lastConstraint) &&
          index == ConstraintEvent.getIndex(lastConstraint)) {
          currentConstraints.add(lastConstraint);
          lastConstraint = null;
        }
      }
      while (constraintIterator.hasNext()) {
        String[] constraint = constraintIterator.next();
        if (jobID == ConstraintEvent.getJobID(constraint) && index == ConstraintEvent.getIndex(constraint)) {
          currentConstraints.add(constraint);
        } else {
          lastConstraint = constraint;
          break;
        }
      }
      Measurements.constraintEvents += currentConstraints.size();
      if (currentConstraints.size() > 0) {
        Measurements.constrainedTasksCount++;
      } else {
        Measurements.freeTasksCount++;
      }

      // 2. App should schedule task
      LOG.debug("Application {} with cellState {} is going to schedule task {}", app, applications.get(app).getCellState(), event);
      boolean isScheduled = applications.get(app).schedule(event.getEvent(), currentConstraints);
      LOG.debug("The result was {}. Updated cellState {}", isScheduled, applications.get(app).getCellState());

      // 3. Add 'end' event
      // timestamp, jobID, index, startTime
      if (isScheduled) {
        if (applications.get(app).getTask(TaskEvent.getJobID(event.getEvent()), TaskEvent.getIndex(event.getEvent())) == null) {
          LOG.error("Scheduled Event not found.");
          throw new RuntimeException("Scheduled Event not found.");
        }
        String[] endEvent = new String[]{
          TaskEvent.getTimestamp(event.getEvent()) + Util.getTaskDuration() + "",
          TaskEvent.getJobID(event.getEvent()) + "",
          TaskEvent.getIndex(event.getEvent()) + "",
          TaskEvent.getTimestamp(event.getEvent()) + ""
        };
        taskEndEvents.add(new Event(endEvent));
      } else {
        Measurements.unscheduledTaskCount++;
      }
    } else {
      Measurements.unconsideredTasks++;
    }
  }

  private static void processEndEvent(Event event) {
    if (event == null) {
      return;
    }
    long jobID = EndEvent.getJobID(event.getEvent());
    int index = EndEvent.getIndex(event.getEvent());
    String app = jobs.get(jobID);
    TaskDiet task = applications.get(app).getTask(jobID, index);
    if (task != null) {
      scheduler.release(task.getMachineID(), task.getMemory(), task.getCpu());
    } else {
      LOG.error("Cannot end a non-running task: " + event + " app:" + app);
      throw new RuntimeException("Cannot end a non-running task: " + event);
    }
    applications.get(app).remove(jobID, index);
  }

  private static void processMachineAttribute(String[] attribute) {
    LOG.debug("Attribute Event {}", Arrays.toString(attribute));
    if (attribute == null) {
      return;
    }
    boolean isDeleted = Integer.parseInt(attribute[4]) == 0 ? false : true;
    if (!isDeleted) {
      cluster.addAttribute(attribute);
    } else {
      cluster.removeAttribute(attribute);
    }
  }

  private static void processMachineEvent(String[] event) {
    LOG.debug("Machine Event {}", Arrays.toString(event));
    if (event == null) {
      return;
    }
    int eventType = Integer.parseInt(event[2]);
    switch (eventType) {
      case MachineEvent.ADD:
        long id = cluster.add(event);
        scheduler.add(id);
        break;
      case MachineEvent.REMOVE:
        cluster.remove(event);
        break;
      case MachineEvent.UPDATE:
        cluster.update(event);
        break;
      default:
        LOG.warn("Unknown Machine Event: {} in {}", eventType, event);
    }
  }

  /**
   * Returns true if there's at least one not null event.
   * @return
   */
  public static boolean keepRolling(Event...events) {
    for (int i=0; i<events.length; i++) {
      if (events[i] != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the next event to be executed. In case of ties, event with smaller index will be returned.
   * Delicate. Handle with care!
   * @param events An array of events.
   * @return Next event. Returns -1 in case of no-event.
   */
  public static int next(Event...events) {
    int next = -1;
    long minTime = Long.MAX_VALUE;
    for (int i=events.length-1 ; i>=0; i--) {
      if (events[i] != null && events[i].getTime() <= minTime) {
        next = i;
        minTime = events[i].getTime();
      }
    }
    return next;
  }
}
