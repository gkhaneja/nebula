package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import edu.illinois.cs.srg.sim.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by gourav on 9/9/14.
 */
public class OmegaSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(OmegaSimulator.class);

  public static Cluster cluster;
  private static OmegaScheduler scheduler;
  private static Map<String, OmegaApplication> applications;

  // Helping variable.
  private static Map<Integer, String[]> lastConstraintEvents;
  private static Queue<Event> taskEndEvents;
  private static Map<Long, String> jobs;

  private static TimeTracker timeTracker = new TimeTracker("edu.illinois.cs.srg.sim.cluster.OmegaSimulator: ");

  public static void main(String[] args) {
    // Constants.DISABLE_RUNTIME_EXCEPTION = true;

    //NebulaConfiguration.init(OmegaSimulator.class.getResourceAsStream(Constants.NEBULA_SITE));
    cluster = new Cluster();
    scheduler = new OmegaScheduler();
    applications = Maps.newHashMap();

    lastConstraintEvents = Maps.newHashMap();
    taskEndEvents = Queues.newPriorityQueue();
    jobs = Maps.newHashMap();

    TimeTracker timeTracker2 = new TimeTracker("edu.illinois.cs.srg.sim.cluster.OmegaSimulator: ");
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
    //TODO:
      //new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> jobIterator = googleTraceReader.open(Constants.JOB_EVENTS);
    Iterator<String[]> taskIterator = googleTraceReader.open(Constants.SUBMIT_TASK_EVENTS, "part-00001-of-[0-9]*.csv");
    Iterator<String[]> attributeIterator = googleTraceReader.open(Constants.MACHINE_ATTRIBUTES);
    Iterator<String[]> machineIterator = googleTraceReader.open(Constants.MACHINE_EVENTS);

    List<Iterator<String[]>> constraintIterators = Lists.newArrayList();
    //TODO: Only got 313 constraints sorted :)  Sort rest of them.
    for (int i=0; i<=313; i++) {
      String pattern = "part-" + String.format("%05d", i) + "-of-00500.csv";
      constraintIterators.add(googleTraceReader.open(Constants.SORTED_TASK_CONSTRAINTS, pattern));
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

      /*if (System.currentTimeMillis() - startTime > 100000 && task != null) {
        startTime = System.currentTimeMillis();
        LOG.info("Current Simulation Time: " + TaskEvent.getTimestamp(task.getEvent()));
        Measurements.print();
        timeTracker.checkpoint();
      }*/

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
          processSubmitTaskEvent(task, constraintIterators);
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
    for (int i=0; i<constraintIterators.size(); i++) {
      if (constraintIterators.get(i).hasNext()) {
        // LOG.error("Inconsistency: There are constraints left to read.");
      }
    }
    Measurements.print();
    LOG.info("No. of total nodes added: " + cluster.getSize());
    //LOG.info("Cluster size: " + Util.getSize(cluster));

  }

  private static void processJobEvent(Event event) {
    if (event == null) {
      return;
    }
    // Only processing SUBMIT events from traces. Other events should come from scheduler.
    if (JobEvent.getEventType(event) == JobEvent.SUBMIT && !jobs.containsKey(JobEvent.getID(event))) {
      Measurements.jobsSubmitted++;
      jobs.put(JobEvent.getID(event), JobEvent.getLogicalName(event));
      String app = JobEvent.getLogicalName(event);
      if (app.equals("")) {
        app = Constants.DEFAULT_JOB_NAME;
      }
      if (!applications.containsKey(app)) {
        applications.put(app, new OmegaApplication(app, scheduler));
      }
    }
  }

  private static void processSubmitTaskEvent(Event event, List<Iterator<String[]>> constraintIterators) {
    //LOG.info("Process Task Event {}", event);
    if (event == null) {
      return;
    }
    long jobID = TaskEvent.getJobID(event.getEvent());
    int index = TaskEvent.getIndex(event.getEvent());

    Measurements.tasksSubmitted++;

    // 1. Get all constraints associated with the task.
    // TODO: What about 19m constraints thing ?
    List<String[]> currentConstraints = Lists.newArrayList();
    for (int i=0; i<constraintIterators.size(); i++) {
      // process constraints from ith file.
      if (lastConstraintEvents.containsKey(i) && lastConstraintEvents.get(i) != null) {
        String[] lastConstraintEvent = lastConstraintEvents.get(i);
        if (jobID == ConstraintEvent.getJobID(lastConstraintEvent) &&
          index == ConstraintEvent.getIndex(lastConstraintEvent)) {
          currentConstraints.add(lastConstraintEvents.remove(i));
        } else {
          continue;
        }
      }
      while (constraintIterators.get(i).hasNext()) {
        String[] constraint = constraintIterators.get(i).next();
        if (jobID == ConstraintEvent.getJobID(constraint) && index == ConstraintEvent.getIndex(constraint)) {
          currentConstraints.add(constraint);
        } else {
          lastConstraintEvents.put(i, constraint);
          break;
        }
      }
    }
    Measurements.constraintEvents += currentConstraints.size();
    if (currentConstraints.size() > 0) {
      Measurements.constrainedTasksCount++;
    } else {
      Measurements.freeTasksCount++;
    }

    // process constraints for current task.
    // 2. App should schedule task
    String app = jobs.get(jobID);
    if (!applications.containsKey(app)) {
      LOG.error("Application {} does not exist: {}", app, event);
      return;
    }
    long startTime = System.currentTimeMillis();
    boolean isScheduled = applications.get(app).schedule(event.getEvent(), currentConstraints);
    if (System.currentTimeMillis() - startTime > 500) {
      LOG.warn("Task Scheduling took " + (System.currentTimeMillis() - startTime) + " ms: " + Measurements.taskEvents);
    }

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
    //LOG.debug("Attribute Event {}", attribute);
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
    //LOG.debug("Machine Event {}", event);
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
  private static boolean keepRolling(Event...events) {
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
  private static int next(Event...events) {
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
