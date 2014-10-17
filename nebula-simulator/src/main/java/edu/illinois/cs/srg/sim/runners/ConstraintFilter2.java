package edu.illinois.cs.srg.sim.runners;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import edu.illinois.cs.srg.sim.task.ConstraintEvent;
import edu.illinois.cs.srg.sim.task.TaskEvent;
import edu.illinois.cs.srg.sim.task.TaskLight;
import edu.illinois.cs.srg.sim.util.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by gourav on 10/7/14.
 */
public class ConstraintFilter2 extends TraceFilter {


  long constrainedTasksCount = 0;
  long totalTaskCount = 0;
  long constraintsCount = 0;

  public ConstraintFilter2(String dir, String source, String target) {
    super(dir, source, target);

  }

  @Override
  void run() {
    TimeTracker timeTracker = new TimeTracker("TraceFilter");
    GoogleTraceReader googleTraceReader = new GoogleTraceReader(dir);
    GoogleTraceIterator sourceIterator = (GoogleTraceIterator) googleTraceReader.open(source, "part-00[0-9][0-9][0-9]-of-[0-9]*.csv");

    String currentFile = sourceIterator.getFile();
    BufferedWriter writer = null;
    HashBasedTable<Long, Integer, HashMap<String, String[]>> tasksConstraints = HashBasedTable.create();
    List<TaskLight> tasks = Lists.newArrayList();

    try {
      writer = openFile(currentFile);
      while (sourceIterator.hasNext()) {
        // Flush file.
        if (currentFile!=sourceIterator.getFile()) {

          getConstraints(tasksConstraints);
          writeup(tasksConstraints, tasks, writer);

          currentFile = sourceIterator.getFile();
          writer.close();
          tasks.clear();
          tasksConstraints.clear();
          LOG.info("{}, {}, {}", constrainedTasksCount, totalTaskCount, constraintsCount);
          writer = openFile(currentFile);
        }

        String[] event = sourceIterator.next();
        long jobID = TaskEvent.getJobID(event);
        int index = TaskEvent.getIndex(event);
        tasksConstraints.put(jobID, index, new HashMap<String, String[]>());
        tasks.add(new TaskLight(jobID, index));
      }
      getConstraints(tasksConstraints);
      writeup(tasksConstraints, tasks, writer);
      writer.close();
    } catch (IOException e) {
      LOG.error("Cannot write to file: " + currentFile, e);
      return;
    }

    LOG.info("{}, {}, {}", constrainedTasksCount, totalTaskCount, constraintsCount);
  }

  private void writeup(HashBasedTable<Long, Integer, HashMap<String, String[]>> tasksConstraints,
                       List<TaskLight> tasks, BufferedWriter writer) {
    StringBuilder content = new StringBuilder();
    try {
      for (TaskLight task : tasks) {
        Collection<String[]> constraints = tasksConstraints.get(task.getJobID(), task.getIndex()).values();
        if (constraints.size() > 0) {
          constrainedTasksCount++;
        }
        totalTaskCount++;
        constraintsCount += constraints.size();
        for (String[] constraint : constraints) {
          content.append(getLine(constraint));
        }
        if (content.length() > 100000000) {
          writer.write(content.toString());
          content.delete(0, content.length());
        }
      }
      writer.write(content.toString());
      content.delete(0, content.length());
    } catch (IOException e) {
      LOG.error("Cannot write to file: ", e);
    }
  }

  @Override
  @Deprecated
  public boolean filter(String[] event) {
    // No-op
    return false;
  }

  public void getConstraints(HashBasedTable<Long, Integer, HashMap<String, String[]>> tasksConstraints) {

    GoogleTraceReader googleTraceReader = new GoogleTraceReader(dir);
    GoogleTraceIterator constraintsIterator = (GoogleTraceIterator) googleTraceReader.open("task_constraints");

    while (constraintsIterator.hasNext()) {
      String[] constraint = constraintsIterator.next();
      long jobId = ConstraintEvent.getJobID(constraint);
      int index = ConstraintEvent.getIndex(constraint);

      if (tasksConstraints.contains(jobId, index)) {
        String name = ConstraintEvent.getName(constraint);
        long constraintTime = ConstraintEvent.getTime(constraint);
        Map<String, String[]> constraints = tasksConstraints.get(jobId, index);
        if (!constraints.containsKey(name) || constraintTime >= ConstraintEvent.getTime(constraints.get(name))) {
          constraints.put(name, constraint);
        }
      }
    }
  }

  public static void main(String[] args) {
    ConstraintFilter2 filter = new ConstraintFilter2(
      "/Users/gourav/projects/googleTraceData/clusterdata-2011-1",
      Constants.SUBMIT_TASK_EVENTS,
      "fuck_constraints");
    filter.run();
  }
}
