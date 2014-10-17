package edu.illinois.cs.srg.sim.experiment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by gourav on 10/17/14.
 */
public class SchedulerServer {
  private static final Logger log = LoggerFactory.getLogger(SchedulerServer.class);

  public static void main(String args[]) throws Exception {

    Cluster.init();
    SchedulerServer server = new SchedulerServer();
    server.serve();
  }

  public void serve() {

    try {
      ServerSocket welcomeSocket = new ServerSocket(Constants.SCHEDULER_PORT);

      while (true) {

        Socket connectionSocket = welcomeSocket.accept();
        Scheduler scheduler = new Scheduler(connectionSocket);
        scheduler.run();

      }
    } catch (Exception e) {
      log.error("SchedulerServer is shutting down.");
      e.printStackTrace();
    }
  }
}
