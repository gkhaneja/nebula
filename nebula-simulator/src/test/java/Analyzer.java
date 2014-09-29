import com.google.common.collect.*;
import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.terminal.PostscriptTerminal;
import edu.illinois.cs.srg.sim.cluster.*;
import edu.illinois.cs.srg.sim.job.JobEvent;
import edu.illinois.cs.srg.sim.job.JobManager;
import edu.illinois.cs.srg.sim.task.ConstraintEvent;
import edu.illinois.cs.srg.sim.task.TaskArrivalComparator;
import edu.illinois.cs.srg.sim.task.TaskEvent;
import edu.illinois.cs.srg.sim.task.TaskLight;
import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.GoogleTraceReader;
import edu.illinois.cs.srg.sim.util.NebulaConfiguration;
import edu.illinois.cs.srg.sim.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by gourav on 9/7/14.
 */
public class Analyzer {
  private static final Logger LOG = LoggerFactory.getLogger(Analyzer.class);

  private static Cluster cluster;
  private static JobManager jobManager;


  public static void main(String[] args) {
    // TODO: This is not working.
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
    // TODO : Not implemented completely.
    Constants.DISABLE_RUNTIME_EXCEPTION = true;

    NebulaConfiguration.init(Analyzer.class.getResourceAsStream(Constants.NEBULA_SITE));
    cluster = new Cluster();
    jobManager = new JobManager();

    //Util.checkpoint();
    //Analyzer.analyzeMachines();
    Analyzer.analyzeResourceRequirements();
    //checkpoint();
  }

  public static void analyzeResourceRequirements() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> taskIterator = googleTraceReader.open(Constants.SUBMIT_TASK_EVENTS);

    Map<Double, Long> cpu = Maps.newHashMap();
    Map<Double, Long> memory = Maps.newHashMap();

    while (taskIterator.hasNext()) {
      String[] task = taskIterator.next();
      Util.increment(cpu, TaskEvent.getCPU(task));
      Util.increment(memory, TaskEvent.getMemory(task));
    }

    LOG.info("CPU: Size: {}, Zeros: {}", cpu.size(), cpu.get(0));
    LOG.info("Memory: Size: {}, Zeros: {}", memory.size(), memory.get(0));
    LOG.info("cpu: {}", cpu);
    LOG.info("memory: {}", memory);

    plotMap(cpu, "cpu");
    plotMap(memory, "memory");

  }

  public static void plotMap(Map<Double, Long> map, String name) {
    double[][] plotData = new double[map.size()][];
    int index = 0;
    for (Map.Entry<Double, Long> entry : map.entrySet()) {
      plotData[index++] = new double[]{entry.getKey(), entry.getValue()};
    }
    name = Constants.HOME_GRAPHS + "/" + name;
    JavaPlot javaPlot = new JavaPlot();
    javaPlot.setTerminal(new PostscriptTerminal(name + ".zipf.eps"));
    javaPlot.addPlot(plotData);
    javaPlot.plot();
  }

  public static void tasksPerJobDistribution() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> taskIterator = googleTraceReader.open(Constants.TASK_EVENTS);

    Map<Long, Long> tasksDistribution = Maps.newHashMap();
    while (taskIterator.hasNext()) {
      String event[] = taskIterator.next();
      Util.increment(tasksDistribution, TaskEvent.getJobID(event));
    }
    LOG.info("Task Distribution #: " + tasksDistribution.size());

    Util.createGraphs(tasksDistribution, "TasksDistOverJob");
  }

  public static void jobsApps() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> jobIterator = googleTraceReader.open(Constants.JOB_EVENTS);
    Map<Long, String> jobs = Maps.newHashMap();
    Map<String, Long> jobsDistribution = Maps.newHashMap();

    while (jobIterator.hasNext()) {
      String event[] = jobIterator.next();
      if (JobEvent.getEventType(event) == JobEvent.SUBMIT && !jobs.containsKey(JobEvent.getID(event))) {
        Util.increment(jobsDistribution, JobEvent.getLogicalName(event));
        jobs.put(JobEvent.getID(event), JobEvent.getLogicalName(event));
      }
    }
    List<App> apps = Lists.newArrayList();
    for (Map.Entry<String, Long> entry : jobsDistribution.entrySet()) {
      apps.add(new App(entry.getKey(), entry.getValue()));
    }
    Collections.sort(apps, new Comparator<App>() {
      @Override
      public int compare(App o1, App o2) {
        return -1*o1.count.compareTo(o2.count);
      }
    });
    for (App app : apps) {
      System.out.println(app);
    }
    //LOG.info("");

    /*long totalJobs = jobs.size();
    List<Long> dist = Util.getZipf(jobsDistribution);
    LOG.info("Job Distribution #: " + dist);

    List<Double> fractions = Lists.newArrayList();
    long sum = 0;
    long significant = -1;
    for (int i=0; i<dist.size(); i++) {
      sum += dist.get(i);
      fractions.add(sum * 1.0 / totalJobs);
      if (sum * 1.0 / totalJobs > 0.7 && significant < 0) {
        significant = i;
      }
    }
    LOG.info("Apps: " + apps.subList(0, 1000));*/
    //LOG.info("Fractions         : " + fractions);
    //LOG.info("Significant: " + significant);
    //Util.createGraphs(jobsDistribution, "JobDistOverApp");
  }

  static class App {
    String name;
    Long count;

    App(String name, long count) {
      this.name = name;
      this.count = count;
    }

    @Override
    public String toString() {
      return name + "," + count;
    }
  }


  public static void analyzeConstraints() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> constraintIterator = googleTraceReader.open(Constants.TASK_CONSTRAINTS);


    List<Long> constraints = Lists.newArrayList();

    while (constraintIterator.hasNext()) {
      String[] event = constraintIterator.next();
      constraints.add(ConstraintEvent.getJobID(event));
    }
    LOG.info("Constraint size: " + constraints.size());
  }

  public static void createConstrainedTaskArrivalOrder() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());

    // Get all the constrained tasks.
    Iterator<String[]> constraintIterator;
    constraintIterator = googleTraceReader.open(Constants.TASK_CONSTRAINTS);
    Set<TaskLight> constrainedTasks = Sets.newHashSet();
    while (constraintIterator.hasNext()) {
      String[] event = constraintIterator.next();
      constrainedTasks.add(new TaskLight(ConstraintEvent.getJobID(event), ConstraintEvent.getIndex(event)));
    }
    Util.checkpoint("Iterated through constraints. Got all constrained tasks.");

    // Get the arrival order.
    List<TaskLight> tasksArrivalOrder = Lists.newArrayList();
    Set<TaskLight> alreadySeenTasks = Sets.newHashSet();
    Iterator<String[]> taskIterator = googleTraceReader.open(Constants.TASK_EVENTS);
    while (taskIterator.hasNext()) {
      String event[] = taskIterator.next();
      TaskLight task = new TaskLight(TaskEvent.getJobID(event), TaskEvent.getIndex(event));
      if (!alreadySeenTasks.contains(task) && constrainedTasks.contains(task)) {
        tasksArrivalOrder.add(task);
        alreadySeenTasks.add(task);
        constrainedTasks.remove(task);
      }
    }
    Util.checkpoint("Iterated through tasks. Got constrained Task arrival order.");
    //LOG.info("Tasks arrival order #: " + tasksArrivalOrder.size());
    //LOG.info("" + tasksArrivalOrder);
  }

  public static void sortConstraints() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());

    // Read constrainedTaskArrivalOrder
    List<TaskLight> tasksArrivalOrder = new ArrayList<TaskLight>();
    String file = "ConstrainedTaskArrivalOrder";
    try {
      BufferedReader reader = new BufferedReader(new FileReader(new File(Util.LOG_HOME + file)));
      String line = reader.readLine();
      reader.close();
      line = line.substring(1, line.length() - 1);
      // LOG.info("line: " + line.substring(0, 1000) + " ~~~~ " + line.substring(line.length() - 1000, line.length() - 1));
      List<String> tasksAsString = new ArrayList<String>(Arrays.asList(line.split(",")));
      for (String taskAsString : tasksAsString) {
        tasksArrivalOrder.add(new TaskLight(taskAsString));
      }
      line = null;
      tasksAsString.clear();
    } catch (IOException e) {
      LOG.error("Cannot write to file: " + Util.LOG_HOME + file, e);
    }
    Util.checkpoint("Read constrainedTaskArrivalOrder.");


    // Sort all files.
    for (int i=314; i<=499; i++) {

      String pattern = "part-" + String.format("%05d", i) + "-of-00500.csv";
      // sort one file 00001.
      List<String[]> constraints = Lists.newArrayList();
      Iterator<String[]> constraintIterator =
        googleTraceReader.open(Constants.TASK_CONSTRAINTS, pattern);
      while (constraintIterator.hasNext()) {
        String[] event = constraintIterator.next();
        constraints.add(event);
      }

      Collections.sort(constraints, new TaskArrivalComparator(tasksArrivalOrder));
      // checkpoint("Created sorted constraints order.");

      // Writing to file.
      Util.print(constraints, pattern);
      constraints.clear();
      Util.checkpoint("Sorted file " + pattern + ".");
    }

  }

  public static void constraintsPerJobDistribution() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> constraintIterator = googleTraceReader.open(Constants.TASK_CONSTRAINTS);
    Map<Long, Long> constraintsPerJob = Maps.newHashMap();

    while (constraintIterator.hasNext()) {
      String[] event = constraintIterator.next();
      Util.increment(constraintsPerJob, ConstraintEvent.getJobID(event));
    }
    LOG.info("ID size: " + constraintsPerJob.size());
    Util.createGraphs(constraintsPerJob, "Constraints-Job");



  }

  public static void constraintsPerTaskDistribution() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> constraintIterator = googleTraceReader.open(Constants.TASK_CONSTRAINTS);
    Table<Long, Integer, Long> tasks = HashBasedTable.create();


    while (constraintIterator.hasNext()) {
      String[] event = constraintIterator.next();
      Util.increment(tasks, ConstraintEvent.getJobID(event), ConstraintEvent.getIndex(event));
    }
    LOG.info("Task size: " + tasks.size());

    List<Long> distribution = new ArrayList<Long>(tasks.values());
    Collections.sort(distribution, new Comparator<Long>() {
      @Override
      public int compare(Long o1, Long o2) {
        return -1 * o1.compareTo(o2);
      }
    });
    Util.createGraphs(distribution, "Constraints-Tasks");



  }

  public static void analyzeJobs() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> jobIterator = googleTraceReader.open(Constants.JOB_EVENTS);
    Map<String, Long> names = Maps.newHashMap();
    Map<String, Long> logicalNames = Maps.newHashMap();
    Map<Long, Long> ids = Maps.newHashMap();
    long lastID = 0;

    while (jobIterator.hasNext()) {
      String[] event = jobIterator.next();
      if (JobEvent.getEventType(event) == JobEvent.SUBMIT) {

      }

      Util.increment(names, JobEvent.getName(event));
      Util.increment(logicalNames, JobEvent.getLogicalName(event));
      Util.increment(ids, JobEvent.getID(event));
      //processJobEvent(event);
    }

    LOG.info("Logical name size: " + logicalNames.size());
    LOG.info("name size: " + names.size());
    LOG.info("ID size: " + ids.size());

    Util.createGraphs(names, "Job-Name");
    Util.createGraphs(logicalNames, "Job-LogicalName");
    Util.createGraphs(ids, "Events-Job");


  }

  public static void analyzeTasks() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> jobIterator = googleTraceReader.open(Constants.JOB_EVENTS, "part-00499-of-[0-9]*.csv");
    Iterator<String[]> taskIterator = googleTraceReader.open(Constants.TASK_EVENTS, "part-00499-of-[0-9]*.csv");
    long numberOfTaskEvents = 0;
    String[] jobEvent = null;
    String[] taskEvent = null;
    if (jobIterator.hasNext()) {
      jobEvent = jobIterator.next();
    }
    if (taskIterator.hasNext()) {
      taskEvent = taskIterator.next();
    }
    while (jobEvent!=null || taskEvent!=null) {
      //LOG.info((jobEvent) + " and " + (taskEvent));
      long jobEventTime = Long.MAX_VALUE;
      if (jobEvent != null) {
        jobEventTime = Long.parseLong(jobEvent[0]);
      }
      long taskEventTime = Long.MAX_VALUE;
      if (taskEvent != null) {
        try {
          taskEventTime = Long.parseLong(taskEvent[0]);
        } catch (NumberFormatException e) {
          // Inconsistency in trace usage of 2^64 - 1 instead of 2^63 - 1 to represent events at the end.
          // LOG.warn("Replacing timestamp from {} to {} in {}", taskEvent[0], Long.MAX_VALUE, taskEvent);
          taskEventTime = Long.MAX_VALUE;
        }
      }
      if (jobEvent != null && jobEventTime <= taskEventTime) {
        processJobEvent(jobEvent);
        jobEvent = null;
        if (jobIterator.hasNext()) {
          jobEvent = jobIterator.next();
        }
      } else {
        numberOfTaskEvents++;
        if (numberOfTaskEvents % 1000 == 0 || numberOfTaskEvents > 133000) {
          LOG.info("Task events #: " + numberOfTaskEvents);
          LOG.info(Arrays.toString(taskEvent));
        }
        processTaskEvent(taskEvent);
        taskEvent = null;
        if (taskIterator.hasNext()) {
          taskEvent = taskIterator.next();
        }
      }
    }
  }


  private static void processJobEvent(String[] jobEvent) {
    if (jobEvent == null) {
      return;
    }
    jobManager.process(jobEvent);
  }

  private static void processTaskEvent(String[] taskEvent) {
    if (taskEvent == null) {
      return;
    }
    jobManager.processTaskEvent(taskEvent, new ArrayList<String[]>());
  }

  public static void analyzeMachines() {
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(NebulaConfiguration.getNebulaSite().getGoogleTraceHome());
    Iterator<String[]> attributeIterator = googleTraceReader.open(Constants.MACHINE_ATTRIBUTES);
    Iterator<String[]> eventIterator = googleTraceReader.open(Constants.MACHINE_EVENTS);

    String[] event = null;
    String[] attribute = null;
    if (eventIterator.hasNext()) {
      event = eventIterator.next();
    }
    if (attributeIterator.hasNext()) {
      attribute = attributeIterator.next();
    }
    while (event!=null || attribute!=null) {

      long eventTime = Long.MAX_VALUE;
      if (event != null) {
        eventTime = Long.parseLong(event[0]);
      }
      long attributeTime = Long.MAX_VALUE;
      if (attribute != null) {
        attributeTime = Long.parseLong(attribute[0]);
      }
      if (eventTime <= attributeTime) {
        processMachineEvent(event);
        event = null;
        if (eventIterator.hasNext()) {
          event = eventIterator.next();
        }
      } else {
        processMachineAttribute(attribute);
        attribute = null;
        if (attributeIterator.hasNext()) {
          attribute = attributeIterator.next();
        }
      }
    }
  }

  private static void processMachineAttribute(String[] attribute) {
    //LOG.info(Arrays.toString(attribute));
    if (attribute == null) {
      return;
    }

    // TODO: [MAJOR] Attribute updates are irrespective of node updates, removal, addition.
    // Solution: Do not remove nodes on REMOVE events but mark them as delete=1. They still may need attribute updates.
    // Ignoring them for now.
    boolean isDeleted = Integer.parseInt(attribute[4]) == 0 ? false : true;
    if (!isDeleted) {
      cluster.addAttribute(attribute);
    } else {
      cluster.removeAttribute(attribute);
    }
  }

  private static void processMachineEvent(String[] event) {
    if (event == null) {
      return;
    }
    int eventType = Integer.parseInt(event[2]);
    switch (eventType) {
      case MachineEvent.ADD:
        cluster.add(event);
        break;
      case MachineEvent.REMOVE:
        cluster.remove(event);
        break;
      case MachineEvent.UPDATE:
        cluster.update(event);
        break;
      default:
        LOG.warn("Unknown Machine Event: {} in {}", eventType, event);
    }
  }
}

