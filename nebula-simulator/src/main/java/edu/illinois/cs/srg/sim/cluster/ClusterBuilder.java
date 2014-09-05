package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
        Map<Long, Node> nodes = Maps.newHashMap();
        int numberOfRepeatedNodes = 0;
        while (iterator.hasNext()) {

            //System.out.println(Arrays.toString(token));
            Node node = new Node(iterator.next());
            if (!nodes.containsKey(node.getId())) {
                nodes.put(node.getId(), node);
            } else {
                numberOfRepeatedNodes++;
            }
        }
        LOG.info("Number of Repeated Nodes: " + numberOfRepeatedNodes);
        return new Cluster(nodes);
    }
}
