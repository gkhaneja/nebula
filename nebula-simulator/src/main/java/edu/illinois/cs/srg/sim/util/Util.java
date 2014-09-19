package edu.illinois.cs.srg.sim.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import edu.illinois.cs.srg.sim.cluster.TaskLight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
}
