package edu.illinois.cs.srg.sim.cluster;

import java.util.List;

/**
 * Created by gourav on 9/21/14.
 */
public interface Application {

  void addJob(String[] job);

  void schedule(String[] task, List<String[]> constraints);

  void endTask(Event event);
}
