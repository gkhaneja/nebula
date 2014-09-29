package edu.illinois.cs.srg.sim.job;

import edu.illinois.cs.srg.sim.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gourav on 9/7/14.
 */
public class JobStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(JobStateMachine.class);

  public static JobState transition(JobState jobState, int jobEvent) {
    if (jobState.equals(JobState.UNSUBMITTED)) {

      if (jobEvent == JobEvent.SUBMIT) {
        return JobState.PENDING;
      }
    } else if (jobState.equals(JobState.PENDING)) {

      switch (jobEvent) {
        case JobEvent.UPDATE_PENDING:
          return JobState.PENDING;
        case JobEvent.SCHEDULE:
          return JobState.RUNNING;
        case JobEvent.FAIL:
        case JobEvent.KILL:
        case JobEvent.LOST:
          //TODO: Inconsistency: Evict / Finish is not a valid transition from PENDING in state machine diagram.
        case JobEvent.EVICT:
        case JobEvent.FINISH:
          return JobState.DEAD;
      }
    } else if (jobState.equals(JobState.RUNNING)) {
      switch (jobEvent) {
        case JobEvent.UPDATE_RUNNING:
          return JobState.RUNNING;
        case JobEvent.EVICT:
        case JobEvent.FAIL:
        case JobEvent.FINISH:
        case JobEvent.KILL:
        case JobEvent.LOST:
          return JobState.DEAD;
      }

    } else if (jobState.equals(JobState.DEAD)) {
      if (jobEvent == JobEvent.SUBMIT) {
        return JobState.PENDING;
      }
    } else if (jobState.equals(JobState.INVALID)) {
      return JobState.INVALID;
    }
    return undefinedState(jobState, jobEvent);
  }

  private static JobState undefinedState(JobState jobState, int jobEvent) {
    if (!Constants.DISABLE_RUNTIME_EXCEPTION) {
      LOG.error("Unknown Job Transition: {} , {}", jobState, jobEvent);
      // throw new RuntimeException("Unknown Job Transition: " + jobState + ", " + jobEvent);
    }
    return JobState.INVALID;
  }
}
