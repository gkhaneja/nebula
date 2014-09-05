package edu.illinois.cs.srg.sim.cluster;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by gourav on 9/3/14.
 */
public class Node {

    private long id;
    private double cpu;
    private double memory;
    private String platformID;
    //TODO: Add Attributes. private HashMap<>

    public Node(String[] googleTrace) {
        if (googleTrace.length < 3) {
            throw new RuntimeException("Unknown Google Trace Format: " + Arrays.toString(googleTrace));
        }
        this.id = Long.parseLong(googleTrace[1]);

        if (googleTrace.length > 3 && !googleTrace[3].equals("")) {
            this.platformID = googleTrace[3];
        }
        if (googleTrace.length > 4 && !googleTrace[4].equals("")) {
            this.cpu = Double.parseDouble(googleTrace[4]);
        }
        if (googleTrace.length > 5 && !googleTrace[5].equals("")) {
            this.memory = Double.parseDouble(googleTrace[5]);
        }
    }

    public long getId() {
        return id;
    }

    public double getCpu() {
        return cpu;
    }

    public double getMemory() {
        return memory;
    }

    public String getPlatformID() {
        return platformID;
    }

    public enum MachineEventType {
        ADD (0),
        REMOVE (1),
        UPDATE (2);

        private final int machineEventType;

        MachineEventType(int machineEventType) {
            this.machineEventType = machineEventType;
        }

        public int getMachineEventType() {
            return machineEventType;
        }
    }
}
