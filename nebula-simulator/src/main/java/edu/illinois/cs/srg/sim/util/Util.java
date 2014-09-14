package edu.illinois.cs.srg.sim.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Created by gourav on 9/8/14.
 */
public class Util {
  private static Logger LOG = LoggerFactory.getLogger(Util.class);

  private static final Random random = new Random(System.currentTimeMillis());
  public static final Map<Long, Long> durationStats = Maps.newHashMap();


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
    long duration = 10;
    /*int type = random.nextInt(3);
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
}
