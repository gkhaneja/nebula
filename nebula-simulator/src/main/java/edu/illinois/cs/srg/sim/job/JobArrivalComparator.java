package edu.illinois.cs.srg.sim.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

/**
 * Created by gourav on 9/14/14.
 */
public class JobArrivalComparator implements Comparator<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(JobArrivalComparator.class);
  private List<Long> jobArrivalOrder;

  public JobArrivalComparator(List<Long> jobArrivalOrder) {
    this.jobArrivalOrder = jobArrivalOrder;
  }

  @Override
  public int compare(Long o1, Long o2) {
    int index1 = jobArrivalOrder.indexOf(o1);
    int index2 = jobArrivalOrder.indexOf(o2);
    if (index1 == -1 && index2 == -1) {
      LOG.warn("Cannot compare non-existent job: {}, {}", o1, o2);
      return 0;
    } else if (index1 == -1) {

      LOG.warn("Cannot compare non-existent job: {}", o1);
      return 1;
    } else if (index2 == -1) {
      LOG.warn("Cannot compare non-existent job: {}", o2);
      return -1;
    } else  if (index1 < index2) {
      return -1;
    } else if (index2 < index1) {
      return 1;
    } else {
      return 0;
    }


  }
}
