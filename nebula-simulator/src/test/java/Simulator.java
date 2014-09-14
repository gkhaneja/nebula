import com.google.common.collect.Queues;
import edu.illinois.cs.srg.sim.cluster.*;
import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.GoogleTraceReader;
import edu.illinois.cs.srg.sim.util.NebulaConfiguration;
import edu.illinois.cs.srg.sim.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;

/**
 * Created by gourav on 9/9/14.
 */
public class Simulator {
  private static final Logger LOG = LoggerFactory.getLogger(Simulator.class);



  private static Cluster cluster;
  private static JobManager jobManager;
  public static Queue<Event> taskEndEvents;

  public static void main(String[] args) {
    // TODO: This is not working.
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
    // TODO : Bad design.
    // Constants.DISABLE_RUNTIME_EXCEPTION = true;

    NebulaConfiguration.init(Simulator.class.getResourceAsStream(Constants.NEBULA_SITE));
    cluster = new Cluster();
    jobManager = new JobManager();
    taskEndEvents = Queues.newPriorityQueue();
    long startTime = System.currentTimeMillis();
    //Simulator.analyzeMachines();
    Simulator.analyzeTasks();
    LOG.info("Time Taken: {} seconds", (System.currentTimeMillis() - startTime) / 1000);
  }

  public static void analyzeTasks() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> jobIterator = googleTraceReader.open(Constants.JOB_EVENTS);
    Iterator<String[]> taskIterator = googleTraceReader.open(Constants.TASK_EVENTS);
    Iterator<String[]> constraintIterator = googleTraceReader.open(Constants.TASK_CONSTRAINTS);

    Event job = null;
    if (jobIterator.hasNext()) {
      job = new Event(jobIterator.next());
    }

    Event task = null;
    if (taskIterator.hasNext()) {
      task = new Event(taskIterator.next());
    }

    Event constraint = null;
    if (constraintIterator.hasNext()) {
      constraint = new Event(constraintIterator.next());
    }

    Event end = taskEndEvents.peek();
    long count = 0;

    while (keepRolling(job, task, constraint, end)) {
      // LOG.info(job + " " + task + " " + end);
      if (++count % 1000000 == 0) {
        // LOG.info(count + " " + job + " " + task + " " + end);
      }

      if (taskEndEvents.size() > 10000) {
        LOG.warn("Currently Active Tasks: " + taskEndEvents.size());
      }

      switch (next(job, task, constraint, end)) {
        case 0:
          Measurements.jobEvents++;
          processJobEvent(job);
          job = null;
          if (jobIterator.hasNext()) {
            job = new Event(jobIterator.next());
          }
          break;
        case 1:
          Measurements.taskEvents++;
          processTaskEvent(task);
          task = null;
          if (taskIterator.hasNext()) {
            task = new Event(taskIterator.next());
          }
          break;
        case 2:
          Measurements.constraintEvents++;
          processTaskConstraint(constraint);
          constraint = null;
          if (constraintIterator.hasNext()) {
            constraint = new Event(constraintIterator.next());
          }
        case 3:
          // process end event
          processEndEvent(end);
          taskEndEvents.poll();
          break;
        default:
          LOG.warn("Unknown event.");
      }
      end = taskEndEvents.peek();
    }

    Measurements.print();
  }



  private static void processTaskConstraint(Event constraint) {
    if (constraint == null) {
      return;
    }
    jobManager.addConstraint(constraint);



  }


  private static void processJobEvent(Event event) {
    if (event == null) {
      return;
    }
    // Only processing SUBMIT events from traces. Other events should come from scheduler.
    if (JobEvent.getEventType(event) == JobEvent.SUBMIT && !jobManager.containsJob(JobEvent.getID(event))) {
      Measurements.jobsSubmitted++;
      jobManager.process(event.getEvent());
    }
  }

  private static void processTaskEvent(Event event) {
    if (event == null) {
      return;
    }
    if (TaskEvent.getEventType(event.getEvent()) == JobEvent.SUBMIT &&
      !jobManager.containsTask(TaskEvent.getJobID(event.getEvent()), TaskEvent.getIndex(event.getEvent()))) {
      Measurements.tasksSubmitted++;
      jobManager.processTaskEvent(event.getEvent());
      // timestamp, jobID, index, startTime
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
    jobManager.processEndTaskEvent(event);
  }

  public static void analyzeMachines() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> attributeIterator = googleTraceReader.open(Constants.MACHINE_ATTRIBUTES);
    Iterator<String[]> eventIterator = googleTraceReader.open(Constants.MACHINE_EVENTS);

    String[] event = null;
    String[] attribute = null;
    if (eventIterator.hasNext()) {
      event = eventIterator.next();
    }
    if (attributeIterator.hasNext()) {
      attribute = attributeIterator.next();
    }
    while (event!=null || attribute!=null) {

      long eventTime = Long.MAX_VALUE;
      if (event != null) {
        eventTime = Long.parseLong(event[0]);
      }
      long attributeTime = Long.MAX_VALUE;
      if (attribute != null) {
        attributeTime = Long.parseLong(attribute[0]);
      }
      if (eventTime <= attributeTime) {
        processMachineEvent(event);
        event = null;
        if (eventIterator.hasNext()) {
          event = eventIterator.next();
        }
      } else {
        processMachineAttribute(attribute);
        attribute = null;
        if (attributeIterator.hasNext()) {
          attribute = attributeIterator.next();
        }
      }
    }
  }

  private static void processMachineAttribute(String[] attribute) {
    //LOG.info(Arrays.toString(attribute));
    if (attribute == null) {
      return;
    }

    // TODO: [MAJOR] Attribute updates are irrespective of node updates, removal, addition.
    // Solution: Do not remove nodes on REMOVE events but mark them as delete=1. They still may need attribute updates.
    // Ignoring them for now.
    boolean isDeleted = Integer.parseInt(attribute[4]) == 0 ? false : true;
    if (!isDeleted) {
      cluster.addAttribute(attribute);
    } else {
      cluster.removeAttribute(attribute);
    }
  }

  private static void processMachineEvent(String[] event) {
    if (event == null) {
      return;
    }
    int eventType = Integer.parseInt(event[2]);
    switch (eventType) {
      case MachineEvent.ADD:
        cluster.add(event);
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
