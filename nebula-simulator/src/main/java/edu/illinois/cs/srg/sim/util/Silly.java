package edu.illinois.cs.srg.sim.util;

import com.google.common.collect.Lists;
import edu.illinois.cs.srg.sim.task.TaskArrivalComparator;
import edu.illinois.cs.srg.sim.task.TaskLight;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by gourav on 10/5/14.
 */
public class Silly {

  public static void main(String[] args) {


    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader(Util.TRACE_HOME);

    // Read constrainedTaskArrivalOrder
    List<TaskLight> tasksArrivalOrder = new ArrayList<TaskLight>();
    String file = "ConstrainedTaskArrivalOrder";
    try {
      BufferedReader reader = new BufferedReader(new FileReader(new File(Util.TRACE_HOME + "/" + file)));
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
      System.out.println("Cannot read from file: " + Util.TRACE_HOME + file);
    }
    Util.checkpoint("Read constrainedTaskArrivalOrder.");


    // Sort all files.
    for (int i=460; i<=499; i++) {

      String pattern = "part-" + String.format("%05d", i) + "-of-00500.csv";
      // sort one file 00001.
      List<String[]> constraints = Lists.newArrayList();
      Iterator<String[]> constraintIterator = googleTraceReader.open(Constants.TASK_CONSTRAINTS, pattern);
      long count = 0;
      while (constraintIterator.hasNext()) {
        String[] event = constraintIterator.next();
        constraints.add(event);
        if (++count > 100000) break;
      }

      System.out.println("Sorting {} constraints: " + constraints.size());
      Collections.sort(constraints, new TaskArrivalComparator(tasksArrivalOrder));
      //checkpoint("Created sorted constraints order.");
      System.out.println("Sorted");


      constraints.clear();

    }

    /*List<Long> list = Lists.newArrayList();
    Random random = new Random();
    long count = 0;

    while (++count < 600000) {
      list.add(random.nextLong());
    }

    Collections.sort(list);
    System.out.println(list);*/

  }
}
