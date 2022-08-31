package org.example;

import java.io.IOException;

public class ReadQuorumThread implements Runnable {
    String key;
    int readQuorum;
    int replicaPosition;
    private volatile String value;
    private volatile String version;

    public String getValue() {
        return value;
    }

    public String getVersion() {
        return version;
    }

    public ReadQuorumThread(int replicaPosition, String key) {
        this.key = key;
        this.replicaPosition= replicaPosition;
    }

    @Override
    public void run() {
        try {
            System.out.println("ReadQuorumThread started and was done by Thread number "+Thread.currentThread().getId());
            String response = Server.sendToPort(replicaPosition, "*get("+key+")",true);
            String[] attribute = response.split(" ");
            value = attribute[0];
            version = attribute[1];

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("ReadQuorumThread finished and was done by Thread number "+Thread.currentThread().getId());
    }

}
