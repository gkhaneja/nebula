package edu.illinois.cs.srg.sim.experiment;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.cluster.Node;
import edu.illinois.cs.srg.sim.cluster.Usage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by gourav on 10/17/14.
 */
public class Cluster {

  public static Map<Long, Node> nodes;
  public static Map<Long, Usage> usage;

  public static Object lock;

  public static void init() {
    lock = new Object();
    nodes = new HashMap<Long, Node>();
    usage = Maps.newHashMap();
    // create 12K nodes with 0.5 cpu and memory.
    for (long i=0; i<12000; i++) {
      nodes.put(i, new Node(i, 0.5, 0.5));
      usage.put(i, new Usage());
    }
  }

}
