package edu.illinois.cs.srg.sim.experiment;

import edu.illinois.cs.srg.sim.cluster.Node;
import edu.illinois.cs.srg.sim.cluster.Usage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by gourav on 10/17/14.
 */
public class Scheduler implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

  ScheduleRequest request;
  Socket socket;

  public Scheduler(Socket socket) {
    this.socket = socket;
  }

  @Override
  public void run() {
    try {
      ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
      ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
      request = (ScheduleRequest) inputStream.readObject();
      log.info("Received: " + request);


      synchronized (Cluster.lock) {
        for (ScheduleRequest.Task task : request.tasks) {
          // Random first fit.
          long nodeID = -1;
          for (Node node : Cluster.nodes.values()) {
            Usage usage = Cluster.usage.get(node.getId());
            if ((node.getCpu() - usage.cpu >= task.cpu) && (node.getMemory() - usage.memory >= task.memory)) {
              nodeID = node.getId();
              usage.cpu += task.cpu;
              usage.memory += task.memory;
              break;
            }
          }
          log.info("Task {}-{} got scheduled on {}", request.id, task.index, nodeID);
        }
      }
      outputStream.writeObject(new ScheduleResponse(ScheduleResponse.SUCCESS));
      // TODO: how the task will end ?
      inputStream.close();
      outputStream.close();
      socket.close();
    } catch (Exception e) {
      log.error("Discarding request {}", request);
      e.printStackTrace();
    }
  }
}
