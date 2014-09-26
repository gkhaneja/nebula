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
  private OmegaScheduler scheduler;
  private Map<Long, Usage> cellState;
  private HashBasedTable<Long, Integer, TaskDiet> tasks;

  public OmegaApplication(String name, OmegaScheduler scheduler) {

    this.name = name;
    this.scheduler = scheduler;
    //TODO: Change this. Debugging.
    cellState = scheduler.getCellStateReference();

    tasks = HashBasedTable.create();
  }


  @Override
  public boolean schedule(String[] task, List<String[]> constraints) {
    //TODO: Should there be some delay associated here, depending on the application's busyness.
    // TODO: No Gang-scheduling. Scheduling one task a time.
    long jobID = TaskEvent.getJobID(task);
    long startTime;

    startTime = System.currentTimeMillis();
    long node = find(task, constraints);
    if (System.currentTimeMillis() - startTime > 500) {
      LOG.warn("Task finding took " + (System.currentTimeMillis() - startTime) + " ms");
    }
    if (node == -1) {
      Measurements.unscheduledTaskCount++;
      // LOG.warn("Application cannot find a node for the task: {}", task);
      return false;
    }
    Map<Long, Node.Resource> proposal = Maps.newHashMap();
    proposal.put(node, new Node.Resource(TaskEvent.getMemory(task), TaskEvent.getCPU(task)));
    startTime = System.currentTimeMillis();
    OmegaScheduler.TransactionResponse response = scheduler.commit(proposal);
    if (System.currentTimeMillis() - startTime > 500) {
      LOG.warn("Task commit took " + (System.currentTimeMillis() - startTime) + " ms");
    }
    cellState = response.getCellState();
    if (response.getResult().equals(OmegaScheduler.TransactionResult.SUCCESS)) {
      tasks.put(jobID, TaskEvent.getIndex(task), new TaskDiet(node, TaskEvent.getMemory(task), TaskEvent.getCPU(task)));
      return true;
    }
    LOG.error("Transaction failed.");
    Measurements.failedOmegaTransaction++;
    // TODO: Since this is single thread simulator, there will at most one Tx failure here because the app will get
    // most recent CellState. That means, if the above Tx has failed, the next one is surely gonna succeed.
    node = find(task, constraints);
    if (node == -1) {
      //LOG.warn("Application cannot find a node for the task: {}", task);
      Measurements.unscheduledTaskCount++;
      return false;
    }
    proposal.clear();
    proposal.put(node, new Node.Resource(TaskEvent.getMemory(task), TaskEvent.getCPU(task)));
    response = scheduler.commit(proposal);
    cellState = response.getCellState();
    if (response.getResult().equals(OmegaScheduler.TransactionResult.SUCCESS)) {
      tasks.put(jobID, TaskEvent.getIndex(task), new TaskDiet(node, TaskEvent.getMemory(task), TaskEvent.getCPU(task)));
      return true;
    } else {
      LOG.error("Second Tx cannot fail. How come ? You gotta investigate :(");
      return false;
    }
  }

  private long find(String[] task, List<String[]> constraints) {
    long startTime = System.currentTimeMillis();
    Iterator<Long> iterator = cellState.keySet().iterator();
    int count=0;
    while (iterator.hasNext()) {
      count++;
      long id = iterator.next();
      if (match(cellState.get(id), task, constraints, OmegaSimulator.cluster.safeGet(id))) {
        if (System.currentTimeMillis() - startTime > 500) {
          LOG.warn("Task find took " + (System.currentTimeMillis() - startTime) + " ms. Iterations: " + count);
        }
        return id;
      }
    }
    return -1;
  }

  public TaskDiet getTask(long jobID, int index) {
    return tasks.get(jobID, index);
  }

  public void remove(long jobID, int index) {
    tasks.remove(jobID, index);
  }

  private boolean match(Usage usage, String[] task, List<String[]> constraints, Node node) {
    // TODO: Not using machine id, scheduling class, priority, different m/c restriction, disk space request
    // in the task event.
    if (!UsageUtil.check(usage, TaskEvent.getMemory(task), TaskEvent.getCPU(task), node.getMemory(), node.getCpu())) {
      return false;
    }
    for (String[] constraint : constraints) {
      // LOG.error("Constraints should be empty");
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
