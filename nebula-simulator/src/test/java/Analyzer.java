import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.cluster.Cluster;
import edu.illinois.cs.srg.sim.cluster.JobEvent;
import edu.illinois.cs.srg.sim.cluster.JobManager;
import edu.illinois.cs.srg.sim.cluster.MachineEvent;
import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.GoogleTraceReader;
import edu.illinois.cs.srg.sim.util.NebulaConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by gourav on 9/7/14.
 */
public class Analyzer {
  private static final Logger LOG = LoggerFactory.getLogger(Analyzer.class);



  private static Cluster cluster;
  private static JobManager jobManager;

  public static void main(String[] args) {
    // TODO: This is not working.
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
    // TODO : Bad design.
    Constants.DISABLE_RUNTIME_EXCEPTION = true;

    NebulaConfiguration.init(Analyzer.class.getResourceAsStream(Constants.NEBULA_SITE));
    cluster = new Cluster();
    jobManager = new JobManager();

    long startTime = System.currentTimeMillis();
    //Analyzer.analyzeMachines();
    Analyzer.analyzeTasks();
    LOG.info("Time Taken: {} seconds", (System.currentTimeMillis() - startTime) / 1000);
  }

  public static void analyzeJobs() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> jobIterator = googleTraceReader.open(Constants.JOB_EVENTS);

    while (jobIterator.hasNext()) {
      processJobEvent(jobIterator.next());
    }
  }

  public static void analyzeTasks() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> jobIterator = googleTraceReader.open(Constants.JOB_EVENTS, "part-00499-of-[0-9]*.csv");
    Iterator<String[]> taskIterator = googleTraceReader.open(Constants.TASK_EVENTS, "part-00499-of-[0-9]*.csv");
    long numberOfTaskEvents = 0;
    String[] jobEvent = null;
    String[] taskEvent = null;
    if (jobIterator.hasNext()) {
      jobEvent = jobIterator.next();
    }
    if (taskIterator.hasNext()) {
      taskEvent = taskIterator.next();
    }
    while (jobEvent!=null || taskEvent!=null) {
      //LOG.info((jobEvent) + " and " + (taskEvent));
      long jobEventTime = Long.MAX_VALUE;
      if (jobEvent != null) {
        jobEventTime = Long.parseLong(jobEvent[0]);
      }
      long taskEventTime = Long.MAX_VALUE;
      if (taskEvent != null) {
        try {
          taskEventTime = Long.parseLong(taskEvent[0]);
        } catch (NumberFormatException e) {
          // Inconsistency in trace usage of 2^64 - 1 instead of 2^63 - 1 to represent events at the end.
          // LOG.warn("Replacing timestamp from {} to {} in {}", taskEvent[0], Long.MAX_VALUE, taskEvent);
          taskEventTime = Long.MAX_VALUE;
        }
      }
      if (jobEvent != null && jobEventTime <= taskEventTime) {
        processJobEvent(jobEvent);
        jobEvent = null;
        if (jobIterator.hasNext()) {
          jobEvent = jobIterator.next();
        }
      } else {
        numberOfTaskEvents++;
        if (numberOfTaskEvents % 1000 == 0 || numberOfTaskEvents > 133000) {
          LOG.info("Task events #: " + numberOfTaskEvents);
          LOG.info(Arrays.toString(taskEvent));
        }
        processTaskEvent(taskEvent);
        taskEvent = null;
        if (taskIterator.hasNext()) {
          taskEvent = taskIterator.next();
        }
      }
    }
  }

  private static void processJobEvent(String[] jobEvent) {
    if (jobEvent == null) {
      return;
    }
    jobManager.process(jobEvent);
  }

  private static void processTaskEvent(String[] taskEvent) {
    if (taskEvent == null) {
      return;
    }
    jobManager.processTaskEvent(taskEvent);
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


}

