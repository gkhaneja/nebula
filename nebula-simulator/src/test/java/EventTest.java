import com.google.common.collect.Queues;
import edu.illinois.cs.srg.sim.cluster.TaskEndEvent;
import edu.illinois.cs.srg.sim.util.Util;
import org.junit.Test;

import java.util.Queue;

/**
 * Created by gourav on 9/9/14.
 */
public class EventTest {

  @Test
  public void testTaskEndEvents() {
    Queue<TaskEndEvent> taskEndEvents = Queues.newPriorityQueue();
    taskEndEvents.add(new TaskEndEvent(0, 0, 1));
    taskEndEvents.add(new TaskEndEvent(0, 0, 80 * 1000 * 1000 * 60 * 60));
    taskEndEvents.add(new TaskEndEvent(0, 0, 40));

    TaskEndEvent event;
    while ((event = taskEndEvents.poll()) != null) {
      System.out.println(event);
    }
    System.out.println(Util.durationStats);
  }
}
