package edu.illinois.cs.srg.sim.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.terminal.PostscriptTerminal;
import edu.illinois.cs.srg.sim.cluster.TaskLight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.*;

/**
 * Created by gourav on 9/8/14.
 */
public class Util {
  private static Logger LOG = LoggerFactory.getLogger(Util.class);

  private static final Random random = new Random(System.currentTimeMillis());
  public static final Map<Long, Long> durationStats = Maps.newHashMap();

  public static final String LOG_HOME = "/Users/gourav/code/logs/nebula/";

  public static Long parseTimestamp(String timestamp) {
    long time;
    try {
      time = Long.parseLong(timestamp);
    } catch (NumberFormatException e) {
      // Inconsistency in trace usage of 2^64 - 1 instead of 2^63 - 1 to represent events at the end.
      // LOG.warn("Replacing timestamp from {} to {} in {}", timestamp, Long.MAX_VALUE);
      time = Long.MAX_VALUE;
    }
    return time;
  }


  /**
   * Returns random task duration in microseconds.
   * @return
   */
  public static long getTaskDuration() {
    long duration = 1000000;
    int type = random.nextInt(3);
    switch (type) {
      case 0:
        // 1 hour.
        duration = ((long) 1) * 60 * 60 * 1000;
        break;
      case 1:
        // 1 minute.
        duration = 1 * 60 * 1000 * 1000;
        break;
      default:
        // 1 second.
        duration = 1 * 1000 * 1000;
    }
    durationStats.put(duration, (durationStats.containsKey(duration) ? durationStats.get(duration) : 0) + 1);
    //LOG.info("duration, type:  " + duration + ", " + type);*/
    return duration;
  }

  public static <T> void increment(Map<T, Long> map, T key) {
    map.put(key, (map.containsKey(key) ? map.get(key) : 0) + 1);
  }

  public static <R, C> void increment(Table<R, C, Long> table, R row, C column) {
    table.put(row, column, (table.contains(row, column) ? table.get(row, column) : 0) + 1);
  }

  public static <K, V> void printMultimap(Multimap<K, V> multimap, String filename) {

  }

  public static <L> void printList(List<L> list, String filename) {


  }

  public static void print(List<String[]> list, String filename) {
    File file = new File(LOG_HOME + filename);
    if(!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        LOG.error("Cannot create file: " + LOG_HOME + file, e);
        return;
      }
    }
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(file));
      for (String[] entry : list) {
        String line = "";
        for (int i=0; i<entry.length-1; i++) {
          line += entry[i] + ",";
        }
        line += entry[entry.length - 1];
        if (entry.length == 5) {
          line += ",\n";
        } else {
          line += "\n";
        }
        writer.write(line);
      }
      writer.close();
    } catch (IOException e) {
      LOG.error("Cannot write to file: " + LOG_HOME + file, e);
    }
  }

  public static void print(Object object, String sample) {
    File file = new File(LOG_HOME + sample);
    if(!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        LOG.error("Cannot create file: " + LOG_HOME + sample, e);
        return;
      }
    }
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(file));
      writer.write("" + object);
      writer.close();
    } catch (IOException e) {
      LOG.error("Cannot write to file: " + LOG_HOME + sample, e);
    }

  }

  public static long startTime = 0;
  public static MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();


  public static void checkpoint(String message) {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }
    LOG.info(message + " Time Taken: {} seconds", (System.currentTimeMillis() - startTime) / 1000);
    LOG.info("Memory:" + memoryMXBean.getHeapMemoryUsage().toString() + "\n");
    startTime = System.currentTimeMillis();
  }

  public static void checkpoint() {
    checkpoint("");
  }

  /**
   * Returns a sorted list in decreasing order.
   * @param countMap
   * @param <K>
   * @return
   */
  public static <K> List<Long> getZipf(Map<K, Long> countMap) {
    List<Long> sortedCount = new ArrayList<Long>(countMap.values());
    Collections.sort(sortedCount, new Comparator<Long>() {
      @Override
      public int compare(Long o1, Long o2) {
        return -1 * o1.compareTo(o2);
      }
    });
    return sortedCount;
  }

  /**
   * Expects sorted count in a decreasing order.
   * @param sortedCount
   * @return
   */
  public static long[][] getZipfPlotData(List<Long> sortedCount) {
    long[][] plotData = new long[sortedCount.size()][];
    for (int rank=0; rank<sortedCount.size(); rank++) {
      plotData[rank] = new long[]{rank, sortedCount.get(rank)};
    }
    return plotData;
  }

  /**
   * Expects sorted count in a decreasing order.
   * @param sortedCount
   * @return
   */
  public static double[][] getZipfLogPlotData(List<Long> sortedCount) {
    double[][] plotData = new double[sortedCount.size()][];
    for (int rank=0; rank<sortedCount.size(); rank++) {
      plotData[rank] = new double[]{Math.log(rank), Math.log(sortedCount.get(rank))};
    }
    return plotData;
  }

  public static Map<Long, Long> getPareto(List<Long> sortedCount) {
    Map<Long, Long> keyCountPerCount = Maps.newHashMap();
    for (int index=0; index<sortedCount.size(); index++) {
      Util.increment(keyCountPerCount, sortedCount.get(index));
    }
    return keyCountPerCount;
  }

  public static long[][] getParetoPlotData(Map<Long, Long> keyCountPerCount) {
    long[][] plotData = new long[keyCountPerCount.size()][];
    int index = 0;
    for (Map.Entry<Long, Long> entry : keyCountPerCount.entrySet()) {
      plotData[index++] = new long[]{entry.getKey(), entry.getValue()};
    }
    return plotData;
  }

  public static double[][] getParetoLogPlotData(Map<Long, Long> keyCountPerCount) {
    double[][] plotData = new double[keyCountPerCount.size()][];
    int index = 0;
    for (Map.Entry<Long, Long> entry : keyCountPerCount.entrySet()) {
      plotData[index++] = new double[]{Math.log(entry.getKey()), Math.log(entry.getValue())};
    }
    return plotData;
  }

  /**
   * Expects sorted count in a decreasing order.
   * @param sortedCount
   * @return
   */
  public static Map<Long, Double> getPDF(List<Long> sortedCount) {
    if (sortedCount.size() == 0) {
      return new HashMap<Long, Double>();
    }
    int totalKeys = sortedCount.size();
    Map<Long, Double> fractionCoveredByCount = Maps.newHashMap();
    Collections.reverse(sortedCount);
    Long currentCount = sortedCount.get(0);
    for (int i=0; i<sortedCount.size(); i++) {
      if(currentCount != sortedCount.get(i)) {
        fractionCoveredByCount.put(currentCount, (i * 1.0) / totalKeys);
        currentCount = sortedCount.get(i);
      }
    }
    return fractionCoveredByCount;
  }

  public static double[][] getPDFPlotData(Map<Long, Double> fractionCoveredByCount) {
    double[][] plotData = new double[fractionCoveredByCount.size()][];
    int index = 0;
    for (Map.Entry<Long, Double> entry : fractionCoveredByCount.entrySet()) {
      plotData[index++] = new double[]{entry.getKey(), entry.getValue()};
    }
    return plotData;
  }

  // TODO: Add title and legends
  public static <K> void createGraphs(Map<K, Long> counts, String name) {
    List<Long> sortedCount = Util.getZipf(counts);
    createGraphs(sortedCount, name);
  }

  // TODO: Add title and legends
  public static void createGraphs(List<Long> sortedCount, String name) {
    // List<Long> sortedCount = Util.getZipf(counts);
    Map<Long, Long> keyCountPerCount = Util.getPareto(sortedCount);
    name = Constants.HOME_GRAPHS + "/" + name;

    // zipf
    JavaPlot javaPlot = new JavaPlot();
    javaPlot.setTerminal(new PostscriptTerminal(name + ".zipf.eps"));
    javaPlot.addPlot(Util.getZipfPlotData(sortedCount));
    javaPlot.plot();

    //zipf log-log
    javaPlot = new JavaPlot();
    javaPlot.setTerminal(new PostscriptTerminal(name + ".zipf_log.eps"));
    javaPlot.addPlot(Util.getZipfLogPlotData(sortedCount));
    javaPlot.plot();

    // #key per count plot
    javaPlot = new JavaPlot();
    javaPlot.setTerminal(new PostscriptTerminal(name + ".pareto.eps"));
    javaPlot.addPlot(Util.getParetoPlotData(keyCountPerCount));
    javaPlot.plot();

    // #key per count log plot
    javaPlot = new JavaPlot();
    javaPlot.setTerminal(new PostscriptTerminal(name + ".pareto_log.eps"));
    javaPlot.addPlot(Util.getParetoLogPlotData(keyCountPerCount));
    javaPlot.plot();

    // pdf plot
    javaPlot = new JavaPlot();
    javaPlot.setTerminal(new PostscriptTerminal(name + ".pdf.eps"));
    javaPlot.addPlot(Util.getPDFPlotData(Util.getPDF(sortedCount)));
    javaPlot.plot();
  }


}
