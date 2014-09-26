/**
 * Created by gourav on 9/23/14.
 */


import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedInteger;
import edu.illinois.cs.srg.nebula.NebulaAgent;
import edu.illinois.cs.srg.sim.util.TimeTracker;
import edu.illinois.cs.srg.sim.util.Util;

import java.util.Map;

public class Silly {
  /*private static Instrumentation instrumentation;

  public static void premain(String args, Instrumentation inst) {
    instrumentation = inst;
  }

  public static long getObjectSize(Object o) {
    return instrumentation.getObjectSize(o);
  }*/

  public static void main(String[] args) {
    int i = 9;
    double d = 3.0;
    long l = 90;

    System.out.println("int: " + NebulaAgent.getObjectSize(UnsignedInteger.MAX_VALUE));
    //System.out.println("double: " + NebulaAgent.getObjectSize(d));
    //System.out.println("long: " + NebulaAgent.getObjectSize(l));
  }


}
