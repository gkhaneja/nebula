package edu.illinois.cs.srg.sim.app;

import java.util.List;

/**
 * Created by gourav on 9/21/14.
 */
public interface Application {

  //void addJob(String[] job);

  boolean schedule(String[] task, List<String[]> constraints);

  //void end(Event task);
}
