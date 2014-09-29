package edu.illinois.cs.srg.sim.job;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.task.ConstraintEvent;
import edu.illinois.cs.srg.sim.task.EndEvent;
import edu.illinois.cs.srg.sim.util.Event;
import edu.illinois.cs.srg.sim.task.Task;
import edu.illinois.cs.srg.sim.task.TaskEvent;
import edu.illinois.cs.srg.sim.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by gourav on 9/6/14.
 */
public class Job {
  private static final Logger LOG = LoggerFactory.getLogger(Job.class);

  private long id;
  private int missingInfo;
  private JobState state;
  private String username;
  private int schedulingClass;
  private String name;
  private String logicalName;

  private Map<Integer, Task> tasks;

  public Job(String[] job) {
    if (job.length < 4) {
      throw new RuntimeException("Unknown Google Trace Format: " + Arrays.toString(job));
    }
    this.id = Long.parseLong(job[2]);
    this.state = JobState.UNSUBMITTED;
    this.missingInfo = -1;
    this.username = "default";
    this.schedulingClass = -1;
    this.name = "default";
    this.logicalName = "default";

    this.tasks = Maps.newHashMap();
  }

  public long getId() {
    return id;
  }

  public int getMissingInfo() {
    return missingInfo;
  }

  public JobState getState() {
    return state;
  }

  public String getUsername() {
    return username;
  }

  public int getSchedulingClass() {
    return schedulingClass;
  }

  public String getName() {
    return name;
  }

  public String getLogicalName() {
    return logicalName;
  }

  public void update(String[] googleTrace) {

    if (googleTrace.length > 1 && !googleTrace[1].equals("")) {
      this.missingInfo = Integer.parseInt(googleTrace[1]);
    }
    if (googleTrace.length > 3 && !googleTrace[3].equals("")) {
      this.state = JobStateMachine.transition(this.state, Integer.parseInt(googleTrace[3]));
    } else {
      throw new RuntimeException("Missing Job event Type: " +  Arrays.toString(googleTrace));
    }
    if (googleTrace.length > 4 && !googleTrace[4].equals("")) {
      this.username = googleTrace[4];
    }
    if (googleTrace.length > 5 && !googleTrace[5].equals("")) {
      this.schedulingClass = Integer.parseInt(googleTrace[5]);
    }
    if (googleTrace.length > 6 && !googleTrace[6].equals("")) {
      this.name = googleTrace[6];
    }
    if (googleTrace.length > 7 && !googleTrace[7].equals("")) {
      this.logicalName = googleTrace[7];
    }
  }

  public void process(String[] taskEvent) {
      process(taskEvent, null);
  }

  public void process(String[] taskEvent, List<String[]> constraints) {
    int index = TaskEvent.getIndex(taskEvent);
    int eventType = Integer.parseInt(taskEvent[5]);
    if (eventType == JobEvent.SUBMIT && !tasks.containsKey(index)) {
      tasks.put(index, new Task(taskEvent));
    }
    if (!tasks.containsKey(index)) {
      if (!Constants.DISABLE_RUNTIME_EXCEPTION) {
        LOG.error("Cannot process task event for non-existent task " + Arrays.toString(taskEvent));
        throw new RuntimeException("Cannot process task event for non-existent task " + Arrays.toString(taskEvent));
      }
    } else {
      tasks.get(index).update(taskEvent);
      if (constraints != null) {
        tasks.get(index).add(constraints);
      }
      if (tasks.get(index).getState().equals(JobState.DEAD)) {
        tasks.remove(index);
      }
    }
  }

  public void endTask(Event event) {
    if (!tasks.containsKey(EndEvent.getIndex(event.getEvent()))) {
      LOG.error("Cannot end task for a non-existent task: " + event);
      throw new RuntimeException("Cannot end task for a non-existent task: " + event);
    }
    tasks.remove(EndEvent.getIndex(event.getEvent()));
  }

  public boolean containsTask(long index) {
    return tasks.containsKey(index);
  }

  public void addConstraint(Event constraint) {
    if (!tasks.containsKey(ConstraintEvent.getIndex(constraint.getEvent()))) {
      // Measurements.constraintsBeforeTask ++;
      //tasks.put(ConstraintEvent.getIndex(constraint.getEvent()),
        //new Task(ConstraintEvent.getJobID(constraint.getEvent()), ConstraintEvent.getIndex(constraint.getEvent())));
       LOG.error("Cannot add constraint for a non-existent task: " + constraint);
       throw new RuntimeException("Cannot add constraint for a non-existent task: " + constraint);
    } else {
      // Measurements.constraintsAfterTask++;
    }
      tasks.get(ConstraintEvent.getIndex(constraint.getEvent())).add(constraint);
  }

  public Task getTask(int index) {
    return tasks.get(index);
  }
}
