import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import edu.illinois.cs.srg.sim.util.Constants;
import edu.illinois.cs.srg.sim.util.GoogleTraceIterator;
import edu.illinois.cs.srg.sim.util.GoogleTraceReader;
import edu.illinois.cs.srg.sim.util.NebulaSite;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by gourav on 9/4/14.
 */
public class GoogleTraceReaderTest {
    private Gson GSON = new Gson();

    @Test
    public void testIterator() {
        InputStream in = GoogleTraceReaderTest.class.getResourceAsStream(Constants.NEBULA_SITE);
        NebulaSite nebulaSite = GSON.fromJson(
                new JsonReader(
                        new InputStreamReader(in)),
                NebulaSite.class);
        GoogleTraceReader googleTraceReader = new GoogleTraceReader(nebulaSite.getGoogleTraceHome());
        Iterator<String[]> iterator = googleTraceReader.open(Constants.MACHINE_EVENTS);
        long startTime = System.currentTimeMillis();
        long count = 0;
        while (iterator.hasNext()) {
            if (System.currentTimeMillis() - startTime > 5000) {
                break;
            }
            System.out.println(count++ + ":" + Arrays.toString(iterator.next()));

        }
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //TODO: Complete the test: add verifications.
    }
}
