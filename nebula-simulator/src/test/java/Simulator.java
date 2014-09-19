import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.sim.cluster.*;
import edu.illinois.cs.srg.sim.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by gourav on 9/9/14.
 */
public class Simulator {
  private static final Logger LOG = LoggerFactory.getLogger(Simulator.class);

  private static Cluster cluster;
  private static JobManager jobManager;
  private static Map<Integer, String[]> lastConstraintEvents;

  // We no more need seenTasks because we have collected submit tasks already :)
  private static Map<Long, Set<Integer>> seenTasks;

  public static Queue<Event> taskEndEvents;

  public static void main(String[] args) {
    // Constants.DISABLE_RUNTIME_EXCEPTION = true;

    NebulaConfiguration.init(Simulator.class.getResourceAsStream(Constants.NEBULA_SITE));
    cluster = new Cluster();
    jobManager = new JobManager();
    lastConstraintEvents = Maps.newHashMap();
    seenTasks = Maps.newHashMap();

    taskEndEvents = Queues.newPriorityQueue();
    long startTime = System.currentTimeMillis();
    //Simulator.analyzeMachines();
    Simulator.analyzeTasks();
    LOG.info("Time Taken: {} seconds", (System.currentTimeMillis() - startTime) / 1000);
  }

  //TODO: Known Bug: Adds a new line during the start of the file.
  public static void extractSUBMITEvents() {
    TimeTracker timeTracker = new TimeTracker("HOT");
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    GoogleTraceIterator taskIterator = (GoogleTraceIterator) googleTraceReader.open(Constants.TASK_EVENTS);

    StringBuilder content = new StringBuilder();


    String currentFile = taskIterator.getFile();
    BufferedWriter writer = null;

    try {
      writer = openFile(currentFile);
      while (taskIterator.hasNext()) {
        // Reset file.
        if (currentFile!=taskIterator.getFile()) {
          writer.write(content.toString());
          content.delete(0, content.length()-1);
          currentFile = taskIterator.getFile();
          writer.close();
          writer = openFile(currentFile);
        }
        if (content.length() > 100000000) {
          writer.write(content.toString());
          content.delete(0, content.length()-1);
        }
        String[] event = taskIterator.next();
        long jobID = TaskEvent.getJobID(event);
        int index = TaskEvent.getIndex(event);

        if (seenTasks.containsKey(jobID)) {
          if (!seenTasks.get(jobID).contains(index)) {
            seenTasks.get(jobID).add(index);
            content.append(getLine(event));
          }
        } else {
          Set<Integer> indices = Sets.newHashSet();
          indices.add(index);
          seenTasks.put(jobID, indices);
          content.append(getLine(event));
        }
      }
      writer.write(content.toString());
      content.delete(0, content.length() - 1);
      writer.close();
    } catch (IOException e) {
      LOG.error("Cannot write to file: " + currentFile, e);
      return;
    }
  }

  private static BufferedWriter openFile(String currentFile) throws IOException {
    String dir = "/Users/gourav/projects/nebula/task_events/";
    File file = new File(dir + currentFile);
    if (!file.exists()) {
      file.createNewFile();
    }

    return new BufferedWriter(new FileWriter(file));

  }


  private static String getLine(String[] entry) {
    StringBuilder line = new StringBuilder(50);
    for (int i = 0; i < TaskEvent.SIZE; i++) {
      if (i < entry.length) {
        line.append(entry[i]);
      }
      if (i < TaskEvent.SIZE - 1) {
        line.append(",");
      }
    }
    line.append("\n");
    return line.toString();
  }



  public static void analyzeTasks() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> jobIterator = googleTraceReader.open(Constants.JOB_EVENTS);
    Iterator<String[]> taskIterator = googleTraceReader.open(Constants.SUBMIT_TASK_EVENTS);
    // Iterator<String[]> constraintIterator = googleTraceReader.open(Util.LOG_HOME, "", "part-00000-of-00500.csv");

    List<Iterator<String[]>> constraintIterators = Lists.newArrayList();
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

    Event end = taskEndEvents.peek();

    while (keepRolling(job, task, end)) {

      switch (next(job, task, end)) {
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
          processSubmitTaskEvent(task, constraintIterators);
          task = null;
          if (taskIterator.hasNext()) {
            task = new Event(taskIterator.next());
          }
          break;
        case 2:
          // process end event
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
        LOG.error("Inconsistency: There are constraints left to read.");
      }
    }
    Measurements.print();
  }




  // Not used.
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

  private static void processSubmitTaskEvent(Event event, List<Iterator<String[]>> constraintIterators) {
    if (event == null) {
      return;
    }
    long jobID = TaskEvent.getJobID(event.getEvent());
    int index = TaskEvent.getIndex(event.getEvent());
    TaskLight taskLight = new TaskLight(jobID, index);

    // Reading only submit task events.
    //if (TaskEvent.getEventType(event.getEvent()) == JobEvent.SUBMIT &&
    //!jobManager.containsTask(TaskEvent.getJobID(event.getEvent()), TaskEvent.getIndex(event.getEvent()))) {
    // !(seenTasks.containsKey(jobID) && seenTasks.get(jobID).contains(index) )) {
    // seenTasks.add(jobID + "-" + index);

    Measurements.tasksSubmitted++;

    // 1. Add task
    jobManager.processTaskEvent(event.getEvent());

    // 2. Add end event
    // timestamp, jobID, index, startTime
    String[] endEvent = new String[]{
      TaskEvent.getTimestamp(event.getEvent()) + Util.getTaskDuration() + "",
      TaskEvent.getJobID(event.getEvent()) + "",
      TaskEvent.getIndex(event.getEvent()) + "",
      TaskEvent.getTimestamp(event.getEvent()) + ""
    };

    taskEndEvents.add(new Event(endEvent));

    // 3. Add constraints
    // TODO: Add it man, currently only iterating through them, right ?
    // TODO: What about 19m constraints thing ?

    long countOfConstraints=0;
    for (int i=0; i<constraintIterators.size(); i++) {
      // process constraints from ith file.
      if (lastConstraintEvents.containsKey(i) && lastConstraintEvents.get(i) != null) {
        String[] lastConstraintEvent = lastConstraintEvents.get(i);
        if (jobID == ConstraintEvent.getJobID(lastConstraintEvent) &&
          index == ConstraintEvent.getIndex(lastConstraintEvent)) {
          countOfConstraints++;
          lastConstraintEvents.remove(i);


        } else {
          continue;
        }
      }
      while (constraintIterators.get(i).hasNext()) {
        String[] constraint = constraintIterators.get(i).next();
        if (jobID == ConstraintEvent.getJobID(constraint) && index == ConstraintEvent.getIndex(constraint)) {
          countOfConstraints++;
        } else {
          lastConstraintEvents.put(i, constraint);
          break;
        }
      }
    }
    if (countOfConstraints > 0) {
      Measurements.constrainedTasksCount++;
    } else {
      Measurements.freeTasksCount++;
    }

    //} else {
    //LOG.error("Got a task, which was submitted earlier.");
    //}
  }

  private static void processEndEvent(Event event) {
    if (event == null) {
      return;
    }
    /*long jobID = EndEvent.getJobID(event.getEvent());
    int index = EndEvent.getIndex(event.getEvent());
    if (seenTasks.containsKey(jobID)) {
      seenTasks.get(jobID).add(index);
    } else {
      Set<Integer> indices = Sets.newHashSet();
      indices.add(index);
      seenTasks.put(jobID, indices);
    }*/

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
