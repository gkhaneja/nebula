package edu.illinois.cs.srg.sim.runners;

import com.google.common.collect.Queues;
import edu.illinois.cs.srg.sim.omega.OmegaSimulator;
import edu.illinois.cs.srg.sim.task.TaskEvent;
import edu.illinois.cs.srg.sim.util.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Queue;

/**
 * Created by gourav on 10/8/14.
 */
public class EndEventGenerator extends TraceFilter {

  public Queue<Event> taskEndEvents;
  long taskSubmitEvents = 0;
  long endEventCount = 0;

  public EndEventGenerator(String dir, String source, String target) {
    super(dir, source, target);
    taskEndEvents = Queues.newPriorityQueue();
  }

  @Override
  public void run() {
    GoogleTraceReader googleTraceReader = new GoogleTraceReader(dir);
    GoogleTraceIterator taskIterator = (GoogleTraceIterator) googleTraceReader.open(source);

    StringBuilder content = new StringBuilder();

    String currentFile = taskIterator.getFile();
    BufferedWriter writer = null;

    Event task = null;
    if (taskIterator.hasNext()) {
      task = new Event(taskIterator.next());
    }

    Event end = taskEndEvents.peek();

    try {
      writer = openFile(currentFile);
      while (OmegaSimulator.keepRolling(task)) {

        // Reset file.
        if (currentFile!=taskIterator.getFile()) {
          writer.write(content.toString());
          content.delete(0, content.length());
          currentFile = taskIterator.getFile();
          writer.close();
          writer = openFile(currentFile);
        }
        if (content.length() > 100000000) {
          writer.write(content.toString());
          content.delete(0, content.length());
        }


        switch (OmegaSimulator.next(task, end)) {
          case 0:
            processTaskEvent(task);
            task = null;
            if (taskIterator.hasNext()) {
              task = new Event(taskIterator.next());
            }
            break;
          case 1:
            // process end event
            endEventCount++;
            content.append(getLine(end.getEvent()));
            taskEndEvents.poll();
            break;
          default:
            LOG.warn("Unknown event.");
        }
        end = taskEndEvents.peek();

      }
      while (end != null) {
        if (content.length() > 100000000) {
          writer.write(content.toString());
          content.delete(0, content.length());
        }
        endEventCount++;
        content.append(getLine(end.getEvent()));
        taskEndEvents.poll();
        end = taskEndEvents.peek();
      }
      writer.write(content.toString());
      content.delete(0, content.length());
      writer.close();
    } catch (IOException e) {
      LOG.error("Cannot write to file: " + currentFile, e);
      return;
    }

    LOG.info("{}, {}", taskSubmitEvents, endEventCount);
  }

  private void processTaskEvent(Event task) {
    LOG.debug("Process Task Event {}", task);
    if (task == null) {
      return;
    }
    taskSubmitEvents++;
    // 3. Add 'end' event
    // timestamp, jobID, index, startTime, cpu, memory, machine
    String[] endEvent = new String[]{
      TaskEvent.getTimestamp(task.getEvent()) + Util.getTaskDuration() + "",
      TaskEvent.getJobID(task.getEvent()) + "",
      TaskEvent.getIndex(task.getEvent()) + "",
      TaskEvent.getTimestamp(task.getEvent()) + "",
      TaskEvent.getCPU(task.getEvent()) + "",
      TaskEvent.getMemory(task.getEvent()) + "",
      TaskEvent.getMachineID(task.getEvent()) + ""
    };
    taskEndEvents.add(new Event(endEvent));
  }

  @Deprecated
  @Override
  public boolean filter(String[] event) {
    //No-op
    return false;
  }

  public static void main(String[] args) {
    EndEventGenerator endEventGenerator = new EndEventGenerator(
      Util.TRACE_HOME,
      Constants.SUBMIT_TASK_EVENTS,
      "end_events");
    endEventGenerator.run();
  }
}
