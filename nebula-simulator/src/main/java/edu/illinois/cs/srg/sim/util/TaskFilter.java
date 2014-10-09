package edu.illinois.cs.srg.sim.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.sim.task.TaskEvent;

import java.util.Map;
import java.util.Set;

/**
 * HAVE NOT TESTED IT.
 * Created by gourav on 10/6/14.
 */
public class TaskFilter extends TraceFilter {

  private static Map<Long, Set<Integer>> seenTasks;

  public TaskFilter(String dir, String source, String target) {
    super(dir, source, target);
    seenTasks = Maps.newHashMap();
  }

  @Override
  public boolean filter(String[] event) {
    long jobID = TaskEvent.getJobID(event);
    int index = TaskEvent.getIndex(event);

    if (seenTasks.containsKey(jobID)) {
      if (!seenTasks.get(jobID).contains(index)) {
        seenTasks.get(jobID).add(index);
        return true;
      }
    } else {
      Set<Integer> indices = Sets.newHashSet();
      indices.add(index);
      seenTasks.put(jobID, indices);
      return true;
    }
    return false;
  }
}
