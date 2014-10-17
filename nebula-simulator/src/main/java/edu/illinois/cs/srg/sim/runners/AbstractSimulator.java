package edu.illinois.cs.srg.sim.runners;

import edu.illinois.cs.srg.sim.app.DefaultApplication;
import edu.illinois.cs.srg.sim.cluster.Cluster;
import edu.illinois.cs.srg.sim.omega.OmegaSimulator;
import edu.illinois.cs.srg.sim.util.Event;
import edu.illinois.cs.srg.sim.util.GoogleTraceReader;
import edu.illinois.cs.srg.sim.util.Measurements;
import edu.illinois.cs.srg.sim.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by gourav on 10/14/14.
 */
public abstract class AbstractSimulator {
  protected static final Logger LOG = LoggerFactory.getLogger(Simulator.class);

  protected Cluster cluster;
  protected Map<Integer, String[]> lastConstraintEvents;
  protected Map<String, DefaultApplication> applications;


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
      if (collector != null) {
        collector.collect();
      }
    }
    Measurements.print();
  }

  public abstract void simulate();
}
