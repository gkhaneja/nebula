package edu.illinois.cs.srg.sim.util;

import com.google.common.collect.Sets;
import edu.illinois.cs.srg.sim.job.JobEvent;

import java.util.Set;

/**
 * Created by gourav on 10/5/14.
 */
public class JobFilter extends TraceFilter {

  Set<Long> jobs;

  public JobFilter(String dir, String source, String target) {
    super(dir, source, target);
    jobs = Sets.newHashSet();
  }

  @Override
  public boolean filter(String[] event) {
    if (JobEvent.getEventType(event) == JobEvent.SUBMIT && !jobs.contains(JobEvent.getID(event))) {
      jobs.add(JobEvent.getID(event));
      return true;
    }
    return false;
  }

  public static void main(String[] args) {
    JobFilter filter = new JobFilter(
      "/Users/gourav/projects/googleTraceData/clusterdata-2011-1",
      Constants.JOB_EVENTS,
      Constants.SUBMIT_JOB_EVENTS);
    filter.run();
  }
}
