package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by gourav on 9/8/14.
 */
public class JobManager {
  private static final Logger LOG = LoggerFactory.getLogger(JobManager.class);
  private Map<Long, Job> jobs;

  public JobManager() {
    jobs = Maps.newHashMap();
  }

  public void add(String[] job) {

  }

  public void remove(String[] job) {

  }


  public void process(String[] jobEvent) {
    long id = Long.parseLong(jobEvent[2]);
    int eventType = Integer.parseInt(jobEvent[3]);
    if (eventType == JobEvent.SUBMIT && !jobs.containsKey(id)) {
      // create a job object
      jobs.put(id, new Job(jobEvent));
    }
    if (!jobs.containsKey(id)) {
      if (!Constants.DISABLE_RUNTIME_EXCEPTION) {
        LOG.error("Cannot process job event for non-existent job " + Arrays.toString(jobEvent));
        throw new RuntimeException("Cannot process job event for non-existent job " + Arrays.toString(jobEvent));
      }
    } else {
      jobs.get(id).update(jobEvent);
    }
  }

  public void processTaskEvent(String[] taskEvent) {
    long jobID = Long.parseLong(taskEvent[2]);

    if (!jobs.containsKey(jobID)) {
      if (!Constants.DISABLE_RUNTIME_EXCEPTION) {
        LOG.error("Cannot process tasks for non-existent job: " + Arrays.toString(taskEvent));
        // throw new RuntimeException("Cannot process tasks for non-existent job: " + Arrays.toString(taskEvent));
      }
      //TODO: Inconsistency in the trace. There are task events before job events.
      // Since the number of such inconsistencies is not small enough, I'm ignoring this error
      // by creating the job itself.
      jobs.put(jobID, new Job(taskEvent));
    }
    jobs.get(jobID).process(taskEvent);
  }


}