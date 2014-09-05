import edu.illinois.cs.srg.sim.cluster.Cluster;
import edu.illinois.cs.srg.sim.cluster.ClusterBuilder;
import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.NebulaConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gourav on 9/4/14.
 */
public class ClusterBuilderTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterBuilderTest.class);

    @Test
    public void testBuilder() {
        NebulaConfiguration.init(ClusterBuilderTest.class.getResourceAsStream(Constants.NEBULA_SITE));
        long startTime = System.currentTimeMillis();
        Cluster cluster = ClusterBuilder.fromGoogleTraces();
        LOG.info("Time Taken: {} seconds", (System.currentTimeMillis() - startTime)/1000);
    }
}
