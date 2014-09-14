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

  public static int getEventType(String[] event) {
    return Integer.parseInt(event[3]);
  }

  public static int getEventType(Event event) {
    return getEventType(event.getEvent());
  }


  public static String getName(String[] event) {
    return event[6];
  }

  public static String getName(Event event) {
    return getName(event.getEvent());
  }

  public static String getLogicalName(String[] event) {
    return event[7];
  }

  public static String getLogicalName(Event event) {
    return getLogicalName(event.getEvent());
  }


  public static Long getID(String[] event) {
    return Long.parseLong(event[2]);
  }

  public static Long getID(Event event) {
    return getID(event.getEvent());
  }
}
