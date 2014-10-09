package edu.illinois.cs.srg.sim.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.app.DefaultApplication;
import edu.illinois.cs.srg.sim.cluster.*;
import edu.illinois.cs.srg.sim.omega.OmegaSimulator;
import edu.illinois.cs.srg.sim.task.EndEvent;
import edu.illinois.cs.srg.sim.task.TaskEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by gourav on 9/9/14.
 */
public class Simulator {
  private static final Logger LOG = LoggerFactory.getLogger(Simulator.class);

  private Cluster cluster;
  private Map<Integer, String[]> lastConstraintEvents;
  private Map<String, DefaultApplication> applications;

  private TaskProcessor taskProcessor;

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();

    Simulator simulator = new Simulator();
    simulator.simulate();

    LOG.info("Time Taken: {} seconds", (System.currentTimeMillis() - startTime) / 1000);
  }

  public void simulate() {
    cluster = new Cluster();
    lastConstraintEvents = Maps.newHashMap();
    applications = Maps.newHashMap();

    taskProcessor = new TaskProcessor();
    ClusterUtilization collector = new ClusterUtilization();
    String[] files = {Constants.MACHINE_EVENTS, Constants.SUBMIT_TASK_EVENTS, Constants.END_EVENTS};
    Processor[] processors = {new MachineProcessor(), taskProcessor, new EndEventProcessor()};
    simulate(files, processors, collector);

    collector.print();
  }

  public void simulate(String[] files, Processor[] processors, Collector collector) {

    if (files.length != processors.length) {
      LOG.error("files and processor length should be same: {}, {}", Arrays.toString(files), Arrays.toString(processors));
      return;
    }
    int size = files.length;
    GoogleTraceReader googleTraceReader = new GoogleTraceReader(Util.TRACE_HOME);

    Iterator<String[]>[] iterators = new Iterator[size];
    for (int i=0; i<size; i++) {
      iterators[i] = googleTraceReader.open(files[i]);
    }

    Event[] events = new Event[size];
    for (int i=0; i<size; i++) {
      if(iterators[i].hasNext()) {
        events[i] = new Event(iterators[i].next());
      } else {
        events[i] = null;
      }
    }

    while (OmegaSimulator.keepRolling(events)) {

      int index = OmegaSimulator.next(events);

      processors[index].process(events[index].getEvent());
      events[index] = null;
      if (iterators[index].hasNext()) {
        events[index] = new Event(iterators[index].next());
      }
      collector.collect();
    }
    Measurements.print();
  }


  class ClusterUtilization implements Collector {
    Map<String, Long> utils = Maps.newHashMap();
    List<String> utilSeries = Lists.newArrayList();

    public void collect() {
      double util = taskProcessor.cpuRequirement / cluster.cpu;
      DecimalFormat format = new DecimalFormat("#.00");
     String utilStr = format.format(taskProcessor.cpuRequirement / cluster.cpu);
      //Util.increment(utils, util);
      //utilSeries.add(util);
      //System.out.println(cluster.cpu);
      if (util < 0) {
        System.out.println("Util became negative: " + util + ", " + taskProcessor.cpuRequirement + ", " + cluster.cpu);
        System.exit(1);
      }
    }

    public void print() {
      System.out.println(utils);
      System.out.println(utilSeries);
    }
  }


  class MachineProcessor implements Processor {
    @Override
    public void process(String[] event) {
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

  class AttributeProcessor implements Processor {
    @Override
    public void process(String[] attribute) {
      //LOG.info(Arrays.toString(attribute));
      if (attribute == null) {
        return;
      }
      boolean isDeleted = Integer.parseInt(attribute[4]) == 0 ? false : true;
      if (!isDeleted) {
        cluster.addAttribute(attribute);
      } else {
        cluster.removeAttribute(attribute);
      }
    }
  }

  class TaskProcessor implements Processor {
    double cpuRequirement = 0.001;
    double memRequirement = 0.001;

    @Override
    public void process(String[] event) {
      if (event == null) {
        return;
      }
      cpuRequirement += TaskEvent.getCPU(event);
      memRequirement += TaskEvent.getMemory(event);
    }
  }

  class EndEventProcessor implements Processor {

    @Override
    public void process(String[] event) {
      if (event == null) {
        return;
      }
      taskProcessor.cpuRequirement -= EndEvent.getCPU(event);
      taskProcessor.memRequirement -= EndEvent.getMemory(event);
    }
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
        //processMachineEvent(event);
        event = null;
        if (eventIterator.hasNext()) {
          event = eventIterator.next();
        }
      } else {
        //processMachineAttribute(attribute);
        attribute = null;
        if (attributeIterator.hasNext()) {
          attribute = attributeIterator.next();
        }
      }
    }
  }

}
