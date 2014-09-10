package edu.illinois.cs.srg.sim.cluster;

/**
 * Created by gourav on 9/7/14.
 */

/**
 * Also represents TaskEvent.
 */
public class JobEvent {
  public static final int SUBMIT = 0;
  public static final int SCHEDULE = 1;
  public static final int EVICT = 2;
  public static final int FAIL = 3;
  public static final int FINISH = 4;
  public static final int KILL = 5;
  public static final int LOST = 6;
  public static final int UPDATE_PENDING = 7;
  public static final int UPDATE_RUNNING = 8;
}
