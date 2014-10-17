package edu.illinois.cs.srg.sim.runners;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.task.ConstraintEvent;
import edu.illinois.cs.srg.sim.task.TaskEvent;
import edu.illinois.cs.srg.sim.util.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by gourav on 10/5/14.
 */
public class ConstraintFilter extends TraceFilter {

  Map<Integer, String[]> lastConstraintEvents;
  List<Iterator<String[]>> constraintIterators;
  long totalConstraints = 0;
  long constrainedEvents = 0;
  long unconstrainedEvents = 0;

  public ConstraintFilter(String dir, String source, String target) {
    super(dir, source, target);
    lastConstraintEvents = Maps.newHashMap();

    GoogleTraceReader googleTraceReader = new GoogleTraceReader(dir);

    constraintIterators = Lists.newArrayList();
    for (int i=0; i<500; i++) {
      String pattern = "part-" + String.format("%05d", i) + "-of-00[0-9][0-9][0-9].csv";
      try {
        constraintIterators.add(googleTraceReader.open(Constants.SORTED_TASK_CONSTRAINTS, pattern));
      } catch (NoSuchElementException e) {
        LOG.trace("Constraint file pattern {} was not found", pattern);
        // ignore
      }
    }
  }

  @Override
  void run() {
    TimeTracker timeTracker = new TimeTracker("TraceFilter");
    GoogleTraceReader googleTraceReader = new GoogleTraceReader(dir);
    GoogleTraceIterator sourceIterator = (GoogleTraceIterator) googleTraceReader.open(source);

    StringBuilder content = new StringBuilder();

    String currentFile = sourceIterator.getFile();
    BufferedWriter writer = null;

    try {
      writer = openFile(currentFile);
      while (sourceIterator.hasNext()) {
        // Reset file.
        if (currentFile!=sourceIterator.getFile()) {
          writer.write(content.toString());
          content.delete(0, content.length());
          currentFile = sourceIterator.getFile();
          writer.close();
          LOG.info("{}, {}, {}", totalConstraints, constrainedEvents, unconstrainedEvents);
          writer = openFile(currentFile);
        }
        if (content.length() > 100000000) {
          writer.write(content.toString());
          content.delete(0, content.length());
        }
        String[] event = sourceIterator.next();
        Collection<String[]> constraints = getConstraints(new Event(event));

        if (constraints != null) {
          for (String[] constraint : constraints) {
            content.append(getLine(constraint));
          }
        }
      }
      writer.write(content.toString());
      content.delete(0, content.length());
      writer.close();
    } catch (IOException e) {
      LOG.error("Cannot write to file: " + currentFile, e);
      return;
    }

    LOG.info("{}, {}, {}", totalConstraints, constrainedEvents, unconstrainedEvents);
  }

  @Override
  @Deprecated
  public boolean filter(String[] event) {
    // No-op
    return false;
  }

  public Collection<String[]> getConstraints(Event event) {
    if (event == null) {
      return null;
    }
    long jobID = TaskEvent.getJobID(event.getEvent());
    int index = TaskEvent.getIndex(event.getEvent());
    long taskTime = TaskEvent.getTimestamp(event.getEvent());


    Map<String, String[]> currentConstraints = Maps.newHashMap();
    for (int i = 0; i < constraintIterators.size(); i++) {
      // process constraints from ith file.
      if (lastConstraintEvents.containsKey(i) && lastConstraintEvents.get(i) != null) {
        String[] lastConstraintEvent = lastConstraintEvents.get(i);
        if (jobID == ConstraintEvent.getJobID(lastConstraintEvent) &&
          index == ConstraintEvent.getIndex(lastConstraintEvent)) {

          String name = ConstraintEvent.getName(lastConstraintEvent);
          long constraintTime = ConstraintEvent.getTime(lastConstraintEvent);
          if (constraintTime <= taskTime) {
           if (!currentConstraints.containsKey(name) || constraintTime >= ConstraintEvent.getTime(currentConstraints.get(name))) {
             currentConstraints.put(name, lastConstraintEvents.remove(i));
           }
          }
        } else {
          continue;
        }
      }
      while (constraintIterators.get(i).hasNext()) {
        String[] constraint = constraintIterators.get(i).next();
        if (jobID == ConstraintEvent.getJobID(constraint) && index == ConstraintEvent.getIndex(constraint)) {

          String name = ConstraintEvent.getName(constraint);
          long constraintTime = ConstraintEvent.getTime(constraint);
          if (constraintTime <= taskTime) {
            if (!currentConstraints.containsKey(name) || constraintTime >= ConstraintEvent.getTime(currentConstraints.get(name))) {
              currentConstraints.put(name, constraint);
            }
          }
        } else {
          lastConstraintEvents.put(i, constraint);
          break;
        }
      }
    }
    totalConstraints += currentConstraints.size();
    if (currentConstraints.size() > 0) {
      constrainedEvents++;
    } else {
      unconstrainedEvents++;
    }
    return currentConstraints.values();
  }

  public static void main(String[] args) {
    ConstraintFilter filter = new ConstraintFilter(
      "/Users/gourav/projects/googleTraceData/clusterdata-2011-1",
      Constants.SUBMIT_TASK_EVENTS,
      Constants.FILTERED_CONSTRAINED);
    filter.run();
  }
}
