package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by gourav on 9/4/14.
 */
public class Cluster {
    private Map<Long, Node> nodes;

    public Cluster() {
        nodes = Maps.newHashMap();
    }

    public Cluster(Map<Long, Node> nodes) {
        this.nodes = nodes;
    }

    public void addNode(Node node) {
        nodes.put(node.getId(), node);
    }
}
