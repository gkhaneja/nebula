package edu.illinois.cs.srg.sim.util;

import java.util.Random;

/**
 * Created by gourav on 9/4/14.
 */
public class Constants {
  public static final String NEBULA_SITE = "nebula-site.json";
  public static final String GOOGLE_TRACE_FILE_REGEX = "part-?????-of-?????.csv";

  public static final String MACHINE_EVENTS = "machine_events";
  public static final String JOB_EVENTS = "job_events";
  public static final String TASK_EVENTS = "task_events";
  public static final String SUBMIT_TASK_EVENTS = "submit_task_events";
  public static final String MACHINE_ATTRIBUTES = "machine_attributes";
  public static final String TASK_CONSTRAINTS = "task_constraints";
  public static final String SORTED_TASK_CONSTRAINTS = "sorted_task_constraints";
  public static final String APP_EVENTS = "apps";

  public static final String DEBUG_MACHINE_EVENTS = "debug_machine_events";
  public static final String DEBUG_JOB_EVENTS = "debug_job_events";
  public static final String DEBUG_SUBMIT_TASK_EVENTS = "debug_submit_task_events";
  public static final String DEBUG_MACHINE_ATTRIBUTES = "debug_machine_attributes";
  public static final String DEBUG_SORTED_TASK_CONSTRAINTS = "debug_sorted_task_constraints";

  public static final double OS_CPU_FRACTION = 0.2;
  public static final double OS_MEMORY_FRACTION = 0.2;

  // TODO: Not implemented completely.
  public static boolean DISABLE_RUNTIME_EXCEPTION = false;

  public static final String DEFAULT_JOB_NAME = "default";

  public static final String HOME_GRAPHS = "/Users/gourav/projects/googleTraceData/clusterdata-2011-1/graphs";



}
