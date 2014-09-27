package edu.illinois.cs.srg.sim.cluster;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by gourav on 9/26/14.
 */
public class RoundRobinAppFilter {

  String[] apps;
  int next;

  public RoundRobinAppFilter(int appCount) {
    apps = new String[appCount];
    for (int i=0; i<appCount; i++) {
      apps[i] = "CloudApplication-" + i;
    }
    next = 0;
  }

  public String getApp(String[] job) {
    String app = apps[next];
    next = (++next) % apps.length;
    return app;
  }

  public static void main(String[] args) {
    RoundRobinAppFilter appFilter = new RoundRobinAppFilter(10);
    for (int i=0; i<25; i++) {
      System.out.println(appFilter.getApp(new String[3]));
    }
  }
}
