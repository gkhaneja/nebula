package edu.illinois.cs.srg.nebula;

import java.lang.instrument.Instrumentation;

/**
 * Created by gourav on 9/23/14.
 */
public class NebulaAgent {
  private static Instrumentation instrumentation;

  public static void premain(String args, Instrumentation inst) {
    instrumentation = inst;
  }

  public static long getObjectSize(Object o) {
    if (instrumentation == null) {
      // TODO: Add a warning log statement.
      return 0;
    }
    return instrumentation.getObjectSize(o);
  }
}
