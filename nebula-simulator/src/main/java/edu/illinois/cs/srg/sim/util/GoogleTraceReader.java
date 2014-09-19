package edu.illinois.cs.srg.sim.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by gourav on 9/4/14.
 */
public class GoogleTraceReader {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleTraceIterator.class);
  private String home;

  public GoogleTraceReader(String home) {
    this.home = home;
  }

  public Iterator open(String directory) {
    return new GoogleTraceIterator(home + "/" + directory, null);
  }

  public Iterator open(String directory, String file, String pattern) {
    return new GoogleTraceIterator(directory + "/" + file, pattern);
  }

  public Iterator open(String directory, String pattern) {
    return new GoogleTraceIterator(home + "/" + directory, pattern);
  }


}
