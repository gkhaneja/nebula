package edu.illinois.cs.srg.sim.util; /**
 * Created by gourav on 9/23/14.
 */


import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedInteger;
//import edu.illinois.cs.srg.nebula.NebulaAgent;
import edu.illinois.cs.srg.sim.cluster.Usage;
import edu.illinois.cs.srg.sim.util.TimeTracker;
import edu.illinois.cs.srg.sim.util.Util;

import java.util.HashMap;
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
    Map<Long, Usage> map = Maps.newHashMap();
    map.put((long) 0,new Usage());
    map.put((long) 1,new Usage());

    Map<Long, Usage> copy = Maps.newHashMap();
    for (Map.Entry<Long, Usage> entry : map.entrySet()) {
      copy.put(entry.getKey(), new Usage(entry.getValue()));
    }
    map.get((long)0).cpu = 1;
    //usage.cpu = 1;

    System.out.print(copy);

    //System.out.println("int: " + NebulaAgent.getObjectSize(UnsignedInteger.MAX_VALUE));
    //System.out.println("double: " + NebulaAgent.getObjectSize(d));
    //System.out.println("long: " + NebulaAgent.getObjectSize(l));
  }


}
