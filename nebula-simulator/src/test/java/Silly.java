/**
 * Created by gourav on 9/23/14.
 */


import com.google.common.collect.Maps;
import edu.illinois.cs.srg.nebula.NebulaAgent;
import edu.illinois.cs.srg.sim.util.TimeTracker;
import edu.illinois.cs.srg.sim.util.Util;

import java.util.Map;

public class Silly {
  /*private static Instrumentation instrumentation;

  public static void premain(String args, Instrumentation inst) {
    instrumentation = inst;
  }

  public static long getObjectSize(Object o) {
    return instrumentation.getObjectSize(o);
  }*/

  public static void main(String[] args) {
    Map<String, String> map = Maps.newHashMap();
    TimeTracker timeTracker = new TimeTracker("Silly; ");
    //System.out.println(NebulaAgent.getObjectSize(map) + " " + Util.getSize(map));
    map.put("hi", "bye");
    timeTracker.checkpoint();
    //System.out.println(NebulaAgent.getObjectSize(map) + " " + Util.getSize(map));
    for (int i=0; i<1000000; i++) {
      map.put(""+i, ""+i);
    }


    System.out.println(NebulaAgent.getObjectSize(map) + " " + Util.getSize(map));

    timeTracker.checkpoint(" in silly");
    System.out.println(NebulaAgent.getObjectSize(map) + " " + Util.getSize(map));
    //System.out.println(Util.getSize(map.get(0)));
    //System.out.println(Util.getSize(map.get(100000)));

    //System.out.println("hello: " + NebulaAgent.getObjectSize("hello"));
    //System.out.println("hielckneknceocm: " + NebulaAgent.getObjectSize("jffffffffflkofoi3rmfoireiiiiiiiiiieoijvefijvrijrijoifweokmfweklmfeoiwjfiwfwkmfkwmfomfomfowmfkwemfowemfowemfomfi3fmomfierjfuiejrurtgunriufirfmorfmoerifjierjfrifmekrfmeromfiernfiernfejrnfjernfijernfirnfnjrfneirnferifnernferjfnerjfninr"));
    //timeTracker.checkpoint();
  }


}
