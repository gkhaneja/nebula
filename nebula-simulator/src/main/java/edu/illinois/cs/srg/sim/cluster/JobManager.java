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
    long id = JobEvent.getID(jobEvent);
    int eventType = JobEvent.getEventType(jobEvent);
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
    long jobID = TaskEvent.getJobID(taskEvent); //Long.parseLong(taskEvent[2]);

    if (!jobs.containsKey(jobID)) {
      if (!Constants.DISABLE_RUNTIME_EXCEPTION) {
        // LOG.error("Cannot process tasks for non-existent job: " + Arrays.toString(taskEvent));
        // throw new RuntimeException("Cannot process tasks for non-existent job: " + Arrays.toString(taskEvent));
      }
      //TODO: Inconsistency in the trace. There are task events before job events.
      // Since the number of such inconsistencies is not small enough, I'm ignoring this error
      // by creating the job itself.

      jobs.put(jobID, new Job(taskEvent));
    }
    jobs.get(jobID).process(taskEvent);
  }


  public void processEndTaskEvent(Event event) {
    if (!jobs.containsKey(EndEvent.getJobID(event.getEvent()))) {
      LOG.error("Cannot end task for a non-existent job: " + event);
      throw new RuntimeException("Cannot end task for a non-existent job: " + event);
    }
    jobs.get(EndEvent.getJobID(event.getEvent())).endTask(event);

  }

  public boolean containsJob(Long id) {
    return jobs.containsKey(id);
  }

  public boolean containsTask(long jobID, long index) {
    return jobs.containsKey(jobID) && jobs.get(jobID).containsTask(index);
  }

  public void addConstraint(Event constraint) {
    if (!jobs.containsKey(ConstraintEvent.getJobID(constraint.getEvent()))) {
      LOG.error("Cannot add constraint for a non-existent job: " + constraint);
      // throw new RuntimeException("Cannot add constraint for a non-existent job: " + constraint);
    } else {
      jobs.get(ConstraintEvent.getJobID(constraint.getEvent())).addConstraint(constraint);
    }


  }
}
