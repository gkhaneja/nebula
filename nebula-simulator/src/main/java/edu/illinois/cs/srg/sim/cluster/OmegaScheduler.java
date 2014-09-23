package edu.illinois.cs.srg.sim.cluster;

import java.util.Map;

/**
 * Created by gourav on 9/21/14.
 */
public class OmegaScheduler {

  enum TransactionResult {
    SUCCESS,
    NODE_NOT_FOUND,
    RESOURCE_OVERFLOW
  }

  // TODO: As soon as the task comes, app makes a commit Tx. Not simulating simultaneous arrival of commit calls from
  // more than one app.
  // TODO: Resolving conflicts based on priority. Preemption.

  // TODO: create a type CellState ?
  private Cluster cellState;
  /**
   * Initialize scheduler by providing the cluster.
   * cluster is not a copy but a reference to the Scheduler.
   * Scheduler will only modify the resource usage and not the resource type and quantity.
   *
   * @param cellState
   */
  public OmegaScheduler(Cluster cellState) {
    this.cellState = cellState;
  }

  public TransactionResponse commit(Map<Long, Node.Resource> proposals) {
    //TODO: Assuming all Tx are all-or-nothing (atomic, and not incremental).
    // TODO: Not a thread safe method. This method should be executed atomically.
    //TODO: Trade-off between 'looping twice with two copies' and 'looping once with three copies'.
    //      Currently, looping once with three copies.

    Cluster copy = cellState.copyOf();
    for (Map.Entry<Long, Node.Resource> proposal : proposals.entrySet()) {
      long id = proposal.getKey();
      Node.Resource resource = proposal.getValue();
      if (copy.safeContains(id)) {
        if (copy.safeGet(id).getUsage().check(resource.getMemory(), resource.getCpu())) {
          copy.safeGet(id).getUsage().add(resource.getMemory(), resource.getCpu());
        } else {
          return new TransactionResponse(cellState.copyOf(), TransactionResult.RESOURCE_OVERFLOW);
        }
      } else {
        return new TransactionResponse(cellState.copyOf(), TransactionResult.NODE_NOT_FOUND);
      }
    }
    cellState = copy;
    return new TransactionResponse(copy, TransactionResult.SUCCESS);
  }

  // tODO: Change this. First test the basic version.
  public void release(long nodeID, Node.Resource resource) {
    cellState.release(nodeID, resource);
  }

  public class TransactionResponse {
    private Cluster cellState;
    private TransactionResult result;

    public TransactionResponse(Cluster cellState, TransactionResult result) {
      this.cellState = cellState;
      this.result = result;
    }

    public Cluster getCellState() {
      return cellState;
    }

    public TransactionResult getResult() {
      return result;
    }
  }

  public Cluster getCellState() {
    return cellState.copyOf();
  }

}
