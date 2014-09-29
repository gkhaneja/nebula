package edu.illinois.cs.srg.sim.omega;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.cluster.*;
import edu.illinois.cs.srg.sim.util.Measurements;
import edu.illinois.cs.srg.sim.util.UsageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by gourav on 9/21/14.
 */
public class OmegaScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(OmegaScheduler.class);

  enum TransactionResult {
    SUCCESS,
    NODE_NOT_FOUND,
    INVALID, RESOURCE_OVERFLOW
  }

  // TODO: As soon as the task comes, app makes a commit Tx. Not simulating simultaneous arrival of commit calls from
  // more than one app.
  // TODO: Resolving conflicts based on priority. Preemption.

  private Map<Long, Usage> cellState;

  /**
   * Initialize scheduler by providing the cluster.
   * cluster is not a copy but a reference to the Scheduler.
   * Scheduler will only modify the resource usage and not the resource type and quantity.
   *
   */
  public OmegaScheduler() {
    cellState = Maps.newHashMap();
    initializeUsage();

  }

  private void initializeUsage() {
    Iterator<Node> iterator = OmegaSimulator.cluster.getIterator();
    while (iterator.hasNext()) {
      cellState.put(iterator.next().getId(), new Usage());
    }
  }

  public void add(long id) {
    cellState.put(id, new Usage());
  }

  public TransactionResponse commit(Map<Long, Node.Resource> proposals, boolean returnClusterCopy) {
    Measurements.totalOmegaTransaction++;

    //TODO: Assuming all Tx are all-or-nothing (atomic, and not incremental).
    // TODO: Not a thread safe method. This method should be executed atomically.
    //TODO: Trade-off between 'looping twice with two copies' and 'looping once with three copies'.
    //      Currently, looping once with three copies.
    // Assumption: Only one proposal
    if (proposals.size() > 1) {
      LOG.error("Proposal size should be smaller that one: {}", proposals);
      return new TransactionResponse(getCellState(returnClusterCopy), TransactionResult.INVALID);
    }

    //TODO: Expectation of only one proposal to avoid making another copy.
    //Cluster copy = cellState.copyOf();
    for (Map.Entry<Long, Node.Resource> proposal : proposals.entrySet()) {
      long id = proposal.getKey();
      Node.Resource resource = proposal.getValue();
      if (OmegaSimulator.cluster.safeContains(id)) {
        if (UsageUtil.check(cellState.get(id), resource.getMemory(), resource.getCpu(),
          OmegaSimulator.cluster.safeGet(id).getMemory(), OmegaSimulator.cluster.safeGet(id).getCpu())) {
          UsageUtil.add(cellState.get(id), resource.getMemory(), resource.getCpu());
        } else {
          //TODO: Ref
          return new TransactionResponse(getCellState(returnClusterCopy), TransactionResult.RESOURCE_OVERFLOW);
        }
      } else {
        return new TransactionResponse(getCellState(returnClusterCopy), TransactionResult.NODE_NOT_FOUND);
      }
    }
    //cellState = cellState;
    return new TransactionResponse(getCellState(returnClusterCopy), TransactionResult.SUCCESS);
  }

  public class TransactionResponse {
    private Map<Long, Usage> cellState;
    private TransactionResult result;

    public TransactionResponse(Map<Long, Usage> cellState, TransactionResult result) {
      this.cellState = cellState;
      this.result = result;
    }

    public Map<Long, Usage> getCellState() {
      return cellState;
    }

    public TransactionResult getResult() {
      return result;
    }
  }

  public Map<Long, Usage> getCellState(boolean seriously) {
    if (!seriously) {
      return null;
    }
    if (OmegaConf.COPY_CELL_STATE) {
      Map<Long, Usage> copy = Maps.newHashMap();
      for (Map.Entry<Long, Usage> entry : cellState.entrySet()) {
        copy.put(entry.getKey(), new Usage(entry.getValue()));
      }
      return copy;
      //return new HashMap<Long, Usage>(cellState);
    } else {
      return cellState;
    }
  }

  @Deprecated
  public Map<Long, Usage> getCellStateReference() {
    return cellState;
  }

  public void release(long machineID, double memory, double cpu) {
    cellState.get(machineID).memory -= memory;
    cellState.get(machineID).cpu -= cpu;
  }

}
