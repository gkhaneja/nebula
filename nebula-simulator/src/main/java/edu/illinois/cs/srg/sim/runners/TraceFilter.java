package edu.illinois.cs.srg.sim.runners;

import edu.illinois.cs.srg.sim.util.GoogleTraceIterator;
import edu.illinois.cs.srg.sim.util.GoogleTraceReader;
import edu.illinois.cs.srg.sim.util.TimeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by gourav on 10/5/14.
 */
public abstract class TraceFilter {
  protected static Logger LOG = LoggerFactory.getLogger(TraceFilter.class);

  String dir;
  String source;
  String target;

  public TraceFilter(String dir, String source, String target) {
    this.dir = dir;
    this.source = source;
    this.target = target;
  }

  public abstract boolean filter(String[] event);

  void run() {
    TimeTracker timeTracker = new TimeTracker("TraceFilter");
    GoogleTraceReader googleTraceReader = new GoogleTraceReader(dir);
    GoogleTraceIterator sourceIterator = (GoogleTraceIterator) googleTraceReader.open(source);

    StringBuilder content = new StringBuilder();

    String currentFile = sourceIterator.getFile();
    BufferedWriter writer = null;

    try {
      writer = openFile(currentFile);
      while (sourceIterator.hasNext()) {
        // Reset file.
        if (currentFile!=sourceIterator.getFile()) {
          writer.write(content.toString());
          content.delete(0, content.length());
          currentFile = sourceIterator.getFile();
          writer.close();
          writer = openFile(currentFile);
        }
        if (content.length() > 100000000) {
          writer.write(content.toString());
          content.delete(0, content.length());
        }
        String[] event = sourceIterator.next();

        if (filter(event)) {
          content.append(getLine(event));
        }
      }
      writer.write(content.toString());
      content.delete(0, content.length());
      writer.close();
    } catch (IOException e) {
      LOG.error("Cannot write to file: " + currentFile, e);
      return;
    }
  }

  protected BufferedWriter openFile(String currentFile) throws IOException {
    // String dir = "/Users/gourav/projects/nebula/task_events/";
    File file = new File(dir + "/" + target +  "/" + currentFile);
    if (!file.exists()) {
      file.createNewFile();
    }
    return new BufferedWriter(new FileWriter(file));
  }

  protected static String getLine(String[] entry) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < entry.length - 1; i++) {
      line.append(entry[i]);
      line.append(",");
    }
    line.append(entry[entry.length - 1]);
    line.append("\n");
    return line.toString();
  }
}
