package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.Maps;
import org.omg.SendingContext.RunTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


//TODO: Assumption: Not using task_usage data for now. It's huge!
//TODO: Interpret two special timestamps: 0 and 2^63 - 1
//TODO: For now, I'm assigning 'random' task durations.
//TODO: Ignoring constraints after tasks for now.
//TODO: Add JavaPlot to pom.xml
//Confirmation: All traces are sorted w.r.t timestamps

// All constraints come after jobs submissions.

/**
 * Created by gourav on 9/4/14.
 */
public class Cluster {
  private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
  private Map<Long, Node> nodes;

  public Cluster() {
    nodes = Maps.newHashMap();
  }

  private Cluster(Map<Long, Node> nodes) {
    this.nodes = nodes;
  }

  public void add(String[] node) {
    long id = Long.parseLong(node[1]);
    if (nodes.containsKey(id) && !nodes.get(id).isDeleted()) {
      LOG.error("Cannot add node while it already exists: {}" + Arrays.toString(node));
      throw new RuntimeException("Cannot add node while it already exists: {}" + Arrays.toString(node));
    } else if (nodes.containsKey(id)) {
      nodes.get(id).unmarkDeleted(node);
    } else {
      nodes.put(id, new Node(node));
    }
  }

  public void remove(String node[]) {
    long id = Long.parseLong(node[1]);
    if (!nodes.containsKey(id) || (nodes.containsKey(id) && nodes.get(id).isDeleted())) {
      LOG.error("Cannot remove non-existent node: " + Arrays.toString(node));
      throw new RuntimeException("Cannot remove non-existent node: " + Arrays.toString(node));
    } else if (nodes.containsKey(id)) {
      nodes.get(id).markDeleted(node);
    }
  }

  public void update(String[] node) {
    long id = Long.parseLong(node[1]);
    if (!nodes.containsKey(id) || nodes.get(id).isDeleted()) {
      LOG.error("Cannot update non-existent node: " + Arrays.toString(node));
      throw new RuntimeException("Cannot update non-existent node: " + Arrays.toString(node));
    } else {
      nodes.get(id).update(node);
    }
  }

  @Deprecated
  public Node get(long id) {
    return nodes.get(id);
  }

  public Node safeGet(long id) {
    if (!nodes.containsKey(id) || nodes.get(id).isDeleted()) {
      LOG.warn("Cannot return non-existent node: " + id);
      throw new RuntimeException("Cannot return non-existent node: " + id);
    }
    return nodes.get(id);
  }

  @Deprecated
  public boolean contains(long id) {
    return nodes.containsKey(id);
  }

  public boolean safeContains(long id) {
    return (nodes.containsKey(id) && !nodes.get(id).isDeleted());
  }

  public void addAttribute(String[] attribute) {
    long id = Long.parseLong(attribute[1]);
    if (!nodes.containsKey(id)) {
      // TODO: Count these errors. Investigate.
      //LOG.error("Cannot add attribute for non-existent node: {}", Arrays.toString(attribute));
      // Do not throw exception. Is this an inconsistency in the trace ? Ignoring it for now.
      return;
    }
    nodes.get(id).addAttribute(attribute[2], attribute[3]);
  }

  public void removeAttribute(String[] attribute) {
    long id = Long.parseLong(attribute[1]);
    if (!nodes.containsKey(id)) {
      // TODO: Count these errors. Investigate.
      //LOG.error("Cannot remove attribute for non-existent node: {}", Arrays.toString(attribute));
      // Do not throw exception. Is this an inconsistency in the trace ? Ignoring it for now.
      return;
    }
    nodes.get(id).removeAttribute(attribute[2]);
  }

  public Cluster copyOf() {
     return new Cluster(new HashMap<Long, Node>(nodes));
  }

  public void release(long nodeID, Node.Resource resource) {
    nodes.get(nodeID).getUsage().release(resource);
  }

  public Iterator<Node> getIterator() {
    return nodes.values().iterator();
  }

}
