package edu.illinois.cs.srg.sim.app;

import com.google.common.collect.Sets;
import edu.illinois.cs.srg.sim.job.JobEvent;
import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.GoogleTraceReader;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by gourav on 9/26/14.
 */
public class SelectiveAppFilter {

  Set<String> apps;

  public SelectiveAppFilter(int lower, int upper) {
    apps = Sets.newHashSet();
    GoogleTraceReader googleTraceReader =
      new GoogleTraceReader("/Users/gourav/projects/googleTraceData/clusterdata-2011-1");
    Iterator<String[]> appIterator = googleTraceReader.open(Constants.APP_EVENTS);
    int index = 0;
    while (appIterator.hasNext()) {
      String app[] = appIterator.next();
      if (index < lower) {
        // No-op
      } else if (index > upper) {
        break;
      } else {
        apps.add(app[0]);
      }
      index++;
    }
  }

  public String getApp(String[] job) {
    String app = JobEvent.getLogicalName(job);
    if (apps.contains(app)) {
      return app;
    }
    return null;
  }
}
