package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.GoogleTraceIterator;
import edu.illinois.cs.srg.sim.util.GoogleTraceReader;
import edu.illinois.cs.srg.sim.util.NebulaConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by gourav on 9/4/14.
 */
public class ClusterBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterBuilder.class);

  public static Cluster fromGoogleTraces() {
    Iterator<String[]> iterator =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome()).open(Constants.MACHINE_EVENTS);


    Table<String, String, Integer> stats = HashBasedTable.create();
    int numberOfRepeatedNodes = 0;
    Map<String, Integer> eventStats = Maps.newHashMap();

    Map<Long, Node> nodes = Maps.newHashMap();
    Cluster cluster = new Cluster();


    while (iterator.hasNext()) {

      // Adding nodes.
      String[] googleTrace = iterator.next();

      eventStats.put(googleTrace[2], (eventStats.containsKey(googleTrace[2]) ? eventStats.get(googleTrace[2]) : 0) + 1);

      Node node = new Node(googleTrace);
      double cpu = node.getCpu();
      double memory = node.getMemory();
      String platform = node.getPlatformID();
      cluster.add(googleTrace);

      if (!nodes.containsKey(node.getId())) {
        nodes.put(node.getId(), node);

        stats.put(platform, cpu + " " + memory,
          (stats.contains(platform, cpu + " " + memory) ? stats.get(platform, cpu + " " + memory) : 0) + 1);
      } else {
        numberOfRepeatedNodes++;
      }


    }
    LOG.info("Number of Repeated Nodes: " + numberOfRepeatedNodes);
    LOG.info("CPU Stats: " + stats);
    LOG.info("Unique machines: " + nodes.size());
    LOG.info("event Stats: " + eventStats);
    return cluster;
  }
}
