package edu.illinois.cs.srg.sim.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

/**
 * Created by gourav on 9/14/14.
 */
public class TaskArrivalComparator implements Comparator<String[]> {
  private static final Logger LOG = LoggerFactory.getLogger(TaskArrivalComparator.class);
  private List<TaskLight> taskArrivalOrder;

  public TaskArrivalComparator(List<TaskLight> taskArrivalOrder) {
    this.taskArrivalOrder = taskArrivalOrder;
  }

  @Override
  public int compare(String[] o1, String[] o2) {
    TaskLight taskLight1 = new TaskLight(ConstraintEvent.getJobID(o1), ConstraintEvent.getIndex(o1));
    TaskLight taskLight2 = new TaskLight(ConstraintEvent.getJobID(o2), ConstraintEvent.getIndex(o2));
    int index1 = taskArrivalOrder.indexOf(taskLight1);
    int index2 = taskArrivalOrder.indexOf(taskLight2);
    if (index1 == -1 && index2 == -1) {
      LOG.warn("Cannot compare non-existent job: {}, {}", o1, o2);
      return 0;
    } else if (index1 == -1) {
      LOG.warn("Cannot compare non-existent job: {}", o1);
      return 1;
    } else if (index2 == -1) {
      LOG.warn("Cannot compare non-existent job: {}", o2);
      return -1;
    } else  if (index1 < index2) {
      return -1;
    } else if (index2 < index1) {
      return 1;
    } else {
      return 0;
    }


  }
}
