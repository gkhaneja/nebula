package edu.illinois.cs.srg.sim.runners;

/**
 * Created by gourav on 10/6/14.
 */
public interface Processor {

  /**
   * Process the given event.
   */
  void process(String[] event);
}
