package edu.illinois.cs.srg.sim.runners;

import edu.illinois.cs.srg.sim.cluster.Cluster;
import edu.illinois.cs.srg.sim.job.JobEvent;
import edu.illinois.cs.srg.sim.task.TaskEvent;
import edu.illinois.cs.srg.sim.util.Constants;

/**
 * Created by gourav on 10/9/14.
 */

public class EndTimeAnalyzer extends Simulator {

  private TaskProcessor taskProcessor;

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();

    Simulator simulator = new EndTimeAnalyzer();
    simulator.simulate();

    LOG.info("Time Taken: {} seconds", (System.currentTimeMillis() - startTime) / 1000);
  }


  @Override
  public void simulate() {
    cluster = new Cluster();

    taskProcessor = new TaskProcessor();
    Collector collector = new StatCollector();
    String[] files = {Constants.TASK_EVENTS};
    Processor[] processors = {taskProcessor};
    simulate(files, processors, collector);

    collector.print();
  }

  class TaskProcessor implements Processor {

    long failCount = 0;
    long finishCount = 0;
    long evictCount = 0;
    long killCount = 0;
    long lostCount = 0;
    long submitCount = 0;
    long scheduleCount = 0;
    long pendingCount = 0;
    long runningCount = 0;

    @Override
    public void process(String[] event) {
      int eventType = TaskEvent.getEventType(event);
      switch(eventType) {
        case JobEvent.SUBMIT: submitCount++; break;
        case JobEvent.SCHEDULE: scheduleCount++; break;
        case JobEvent.UPDATE_PENDING: pendingCount++; break;
        case JobEvent.UPDATE_RUNNING: runningCount++; break;
        case JobEvent.FAIL: failCount++; break;
        case JobEvent.FINISH: finishCount++; break;
        case JobEvent.EVICT: evictCount++; break;
        case JobEvent.KILL: killCount++; break;
        case JobEvent.LOST: lostCount++; break;
      }
    }
  }

  class StatCollector implements Collector {

    @Override
    public void collect() {
      //No-op
    }

    @Override
    public void print() {
      LOG.info("submit:{} schedule:{} running:{} pending:{} finish:{} fail:{} evict:{} kill:{} lost:{}",
        taskProcessor.submitCount,
        taskProcessor.scheduleCount,
        taskProcessor.runningCount,
        taskProcessor.pendingCount,
        taskProcessor.finishCount,
        taskProcessor.failCount,
        taskProcessor.evictCount,
        taskProcessor.killCount,
        taskProcessor.lostCount);
    }
  }
}
