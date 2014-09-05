package edu.illinois.cs.srg.sim.util;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by gourav on 9/4/14.
 */
public class NebulaConfiguration {

    private static NebulaSite nebulaSite;
    private static Gson GSON = new Gson();

    public static void init(InputStream in) {
        if (nebulaSite == null) {
            nebulaSite = GSON.fromJson(
                    new JsonReader(
                            new InputStreamReader(in)),
                    NebulaSite.class);
        }
    }

    public static NebulaSite getNebulaSite() {
        //init();
        return nebulaSite;
    }

}
