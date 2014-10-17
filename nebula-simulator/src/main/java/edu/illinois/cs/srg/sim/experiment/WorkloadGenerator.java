package edu.illinois.cs.srg.sim.experiment;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Set;

/**
 * Created by gourav on 10/17/14.
 */
public class WorkloadGenerator {
  private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);

  String serverIP;

  public WorkloadGenerator(String serverIP) {
    this.serverIP = serverIP;
  }

  public void generate() {
    Set<ScheduleRequest.Task> tasks = Sets.newHashSet();
    tasks.add(new ScheduleRequest.Task(0, 0.25, 0.25));
    tasks.add(new ScheduleRequest.Task(1, 0.25, 0.25));
    long id = 1;
    while (true) {
      sendRequest(new ScheduleRequest(id++, tasks));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void sendRequest(ScheduleRequest request) {
    try {
      Socket clientSocket = new Socket(serverIP, Constants.SCHEDULER_PORT);
      ObjectOutputStream outStream = new ObjectOutputStream(clientSocket.getOutputStream());
      ObjectInputStream inStream = new ObjectInputStream(clientSocket.getInputStream());
      outStream.writeObject(request);
      ScheduleResponse response = (ScheduleResponse) inStream.readObject();

      log.info("Got Response: {}", response);
      outStream.close();
      inStream.close();
      clientSocket.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String args[])  {
    WorkloadGenerator generator = new WorkloadGenerator(args[args.length - 1]);
    generator.generate();
  }
}
