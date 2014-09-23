package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by gourav on 9/21/14.
 */
public class OmegaApplication implements Application {
  private static final Logger LOG = LoggerFactory.getLogger(OmegaApplication.class);
  private String name;
  private Map<Long, Job> jobs;
  private OmegaScheduler scheduler;
  private Cluster cellState;

  //TODO: Change this.
  private HashBasedTable<Long, Integer, Long> runningTasks;


  public OmegaApplication(String name, OmegaScheduler scheduler) {
    this.name = name;
    this.scheduler = scheduler;
    cellState = scheduler.getCellState();
    jobs = Maps.newHashMap();

    runningTasks = HashBasedTable.create();
  }

  @Override
  public void addJob(String[] job) {
    long id = JobEvent.getID(job);
    int eventType = JobEvent.getEventType(job);
    if (eventType == JobEvent.SUBMIT && !jobs.containsKey(id)) {
      jobs.put(id, new Job(job));
    }
    if (!jobs.containsKey(id)) {
      if (!Constants.DISABLE_RUNTIME_EXCEPTION) {
        LOG.error("Cannot process job event for non-existent job " + Arrays.toString(job));
        throw new RuntimeException("Cannot process job event for non-existent job " + Arrays.toString(job));
      }
    } else {
      jobs.get(id).update(job);
    }
  }

  @Override
  public void schedule(String[] task, List<String[]> constraints) {
    //TODO: Should there be some delay associated here, depending on the application's busyness.
    // TODO: No Gang-scheduling. Scheduling one task a time.
    long jobID = TaskEvent.getJobID(task);

    if (!jobs.containsKey(jobID)) {
      // Inconsistency in the trace. There are task events before job events.
      // Since the number of such inconsistencies is not small enough, I'm ignoring this error
      // by creating the job itself.
      jobs.put(jobID, new Job(task));
    }
    jobs.get(jobID).process(task, constraints);

    Node node = find(task, constraints);
    if (node == null) {
      LOG.warn("Application cannot find a node for the task: {}", task);
      return;
    }
    Map<Long, Node.Resource> proposal = Maps.newHashMap();
    proposal.put(node.getId(), new Node.Resource(TaskEvent.getMemory(task), TaskEvent.getCPU(task)));
    OmegaScheduler.TransactionResponse response = scheduler.commit(proposal);
    cellState = response.getCellState();
    if (response.getResult().equals(OmegaScheduler.TransactionResult.SUCCESS)) {
      runningTasks.put(jobID, TaskEvent.getIndex(task), node.getId());
      return;
    }

    // TODO: Since this is single thread simulator, there will at most one Tx failure here because the app will get
    // most recent CellState. That means, if the above Tx has failed, the next one is surely gonna succeed.
    node = find(task, constraints);
    if (node == null) {
      LOG.warn("Application cannot find a node for the task: {}", task);
      return;
    }
    proposal.clear();
    proposal.put(node.getId(), new Node.Resource(TaskEvent.getMemory(task), TaskEvent.getCPU(task)));
    response = scheduler.commit(proposal);
    cellState = response.getCellState();
    if (response.getResult().equals(OmegaScheduler.TransactionResult.SUCCESS)) {
      runningTasks.put(jobID, TaskEvent.getIndex(task), node.getId());
    } else {
      LOG.error("Second Tx cannot fail. How come ? You gotta investigate :(");
    }
  }

  private Node find(String[] task, List<String[]> constraints) {
    Iterator<Node> iterator = cellState.getIterator();
    while (iterator.hasNext()) {
      Node node = iterator.next();
      if (match(node, task, constraints)) {
        return node;
      }
    }
    return null;
  }

  @Override
  public void endTask(Event event) {
    if (!jobs.containsKey(EndEvent.getJobID(event.getEvent()))) {
      LOG.error("Cannot end task for a non-existent job: " + event);
      throw new RuntimeException("Cannot end task for a non-existent job: " + event);
    }

    long jobID = EndEvent.getJobID(event.getEvent());
    int index = EndEvent.getIndex(event.getEvent());

    // TODO: This guy shouldn't be doing this.
    if (runningTasks.contains(jobID, index)) {
      long nodeID = runningTasks.get(jobID, index);
      // TODO: This can replace running tasks.
      Task task = jobs.get(jobID).getTask(index);
      scheduler.release(nodeID, new Node.Resource(task.getMemory(), task.getCpu()));
      // TODO: Not updating local copy :D
    } else {
      LOG.error("Cannot end a non-running task: " + event);
      throw new RuntimeException("Cannot end a non-running task: " + event);
    }

    jobs.get(EndEvent.getJobID(event.getEvent())).endTask(event);

  }

  public boolean containsJob(Long id) {
    return jobs.containsKey(id);
  }

  public boolean containsTask(long jobID, long index) {
    return jobs.containsKey(jobID) && jobs.get(jobID).containsTask(index);
  }

  private boolean match(Node node, String[] task, List<String[]> constraints) {
    // TODO: Not using machine id, scheduling class, priority, different m/c restriction, disk space request
    // in the task event.
    if (!node.getUsage().check(TaskEvent.getMemory(task), TaskEvent.getCPU(task))) {
      return false;
    }
    for (String[] constraint : constraints) {
      String name = ConstraintEvent.getName(constraint);
      String value = node.getAttribute(name);

      String demand = ConstraintEvent.getValue(constraint);
      switch (ConstraintEvent.getOperator(constraint)) {
        case ConstraintEvent.EQUAL:
          String supply1 = (value == null) ? "" : value;
          if (!supply1.equals(demand)) {
            return false;
          }
          break;
        case ConstraintEvent.NOT_EQUAL:
          String supply2 = (value == null) ? "" : value;
          if (supply2.equals(demand)) {
            return false;
          }
          break;
        case ConstraintEvent.LESS_THAN:
          int supply3 = (value == null) ? 0 : Integer.parseInt(value);
          if (supply3 >= Integer.parseInt(demand)) {
            return false;
          }
          break;
        case ConstraintEvent.GREATER_THAN:
          int supply4 = (value == null) ? 0 : Integer.parseInt(value);
          if (supply4 <= Integer.parseInt(demand)) {
            return false;
          }
          break;
        default:
          LOG.error("Ignoring unknown operator in constraint event: {}", constraint);
      }
    }
    return true;
  }
}
