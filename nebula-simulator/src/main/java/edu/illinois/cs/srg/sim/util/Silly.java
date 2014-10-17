package edu.illinois.cs.srg.sim.util;

import com.google.common.collect.Lists;
import edu.illinois.cs.srg.sim.task.TaskArrivalComparator;
import edu.illinois.cs.srg.sim.task.TaskLight;
import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMeans;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.tools.data.FileHandler;

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
    try {
    /* Load a dataset */
      Dataset data = FileHandler.loadDataset(new File("/Users/gourav/Downloads/synthetic_control.data.txt"), " ");
/* Create a new instance of the KMeans algorithm, with no options
  * specified. By default this will generate 4 clusters. */
      Clusterer km = new KMeans();
/* Cluster the data, it will be returned as an array of data sets, with
  * each dataset representing a cluster. */
      Dataset[] clusters = km.cluster(data);
      System.out.println(clusters);
    } catch (Exception e) {
      e.printStackTrace();
    }



  }
}
