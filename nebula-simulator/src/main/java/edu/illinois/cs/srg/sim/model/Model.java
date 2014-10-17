package edu.illinois.cs.srg.sim.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.illinois.cs.srg.sim.job.JobEvent;
import edu.illinois.cs.srg.sim.runners.AbstractSimulator;
import edu.illinois.cs.srg.sim.runners.Processor;
import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.Util;
import net.sf.javaml.clustering.mcl.SparseVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.Canopy;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by gourav on 10/14/14.
 */
public class Model extends AbstractSimulator {

  private JobProcessor jobProcessor;
  Map<Long, Job> jobs;

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();

    if (args.length < 2) {
      LOG.error("Usage: <> <filename>");
      return;
    }

    Model model = new Model();
    //model.simulate();
    //model.print(args[args.length-2], args[args.length-1]);
    model.test(args[args.length-2], args[args.length-1]);
    LOG.info("Time Taken: {} seconds", (System.currentTimeMillis() - startTime) / 1000);
  }

  @Override
  public void simulate() {
    //cluster = new Cluster();
    lastConstraintEvents = Maps.newHashMap();
    applications = Maps.newHashMap();

    jobs = Maps.newHashMap();
    jobProcessor = new JobProcessor();

    String[] files = {Constants.JOB_EVENTS};
    Processor[] processors = {new JobProcessor()};
    simulate(files, processors, null);


  }

  class JobProcessor implements Processor {

    @Override
    public void process(String[] event) {
      long id = JobEvent.getID(event);
      if (!jobs.containsKey(id)) {
        Job job = new Job(event);
        job.update(event);
        jobs.put(id, job);
      }
    }
  }

  public void test(String dir, String filename) {
    List<double[]> vectors = Lists.newArrayList();
    for (int i=0;i<10;i++) vectors.add(new double[]{0});
    for (int i=0;i<10;i++) vectors.add(new double[]{1});
    for (int i=0;i<5;i++) vectors.add(new double[]{2});
    for (int i=0;i<1;i++) vectors.add(new double[]{3});
    writeVectors(dir, "vectors", filename, vectors);

    Map<Integer, double[]> initClusters = Maps.newHashMap();
    initClusters.put(0, new double[]{0});
    initClusters.put(2, new double[]{2});
    initClusters.put(3, new double[]{3});
    writeClusters(dir, "init", filename, initClusters);
  }

  public void writeVectors(String dir, String prefix, String filename, List<double[]> vectors) {
    try {
      Configuration configuration = new Configuration();
      FileSystem fileSystem = FileSystem.get(configuration);

      SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem, configuration, new Path(dir + "/seq/"  + prefix + "/" + filename), Text.class, VectorWritable.class);
      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(dir + "/txt/"  + prefix + "/" + filename)));
      VectorWritable vectorWritable = new VectorWritable();
      for (double[] entry : vectors) {
        DenseVector vector = new DenseVector(entry);
        vectorWritable.set(vector);
        writer.append(new Text(""), vectorWritable);
        String str = Arrays.toString(entry);
        bufferedWriter.write(str.substring(1,str.length()-1));
        bufferedWriter.newLine();
      }
      writer.close();
      bufferedWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void writeClusters(String dir, String prefix, String filename, Map<Integer, double[]> vectors) {
    try {
      Configuration configuration = new Configuration();
      FileSystem fileSystem = FileSystem.get(configuration);

      SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem, configuration, new Path(dir + "/seq/"  + prefix + "/" + filename), Text.class, ClusterWritable.class);
      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(dir + "/txt/"  + prefix + "/" + filename)));
      //VectorWritable vectorWritable = new VectorWritable();
      ClusterWritable clusterWritable = new ClusterWritable();
      for (Map.Entry<Integer, double[]> entry : vectors.entrySet()) {
        DenseVector vector = new DenseVector(entry.getValue());
        Canopy canopy = new Canopy(vector, entry.getKey(), new SquaredEuclideanDistanceMeasure());
        Cluster cluster = new Kluster(vector, entry.getKey(), new SquaredEuclideanDistanceMeasure());
        clusterWritable.setValue(canopy);
        writer.append(new Text(), clusterWritable);
        String str = Arrays.toString(entry.getValue());
        bufferedWriter.write(entry.getKey() + "," +  str.substring(1,str.length()-1));
        bufferedWriter.newLine();
      }
      writer.close();
      bufferedWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void print(String dir, String filename) {
    try {
      Configuration configuration = new Configuration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Map<Integer, Long> classes = Maps.newHashMap();

      SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem, configuration, new Path(dir + "/seq/" + filename), Text.class, VectorWritable.class);
      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(dir + "/txt/" + filename)));
      VectorWritable vectorWritable = new VectorWritable();
      for (Job job : jobs.values()) {
        DenseVector vector = new DenseVector(new double[]{job.getSchedulingClass()});
        vectorWritable.set(vector);
        writer.append(new Text(job.getSchedulingClass() + ""), vectorWritable);
        bufferedWriter.write(job.getSchedulingClass() + "");
        bufferedWriter.newLine();
        Util.increment(classes, job.getSchedulingClass());
      }
      writer.close();
      bufferedWriter.close();
      System.out.println("Classes: " + classes);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
