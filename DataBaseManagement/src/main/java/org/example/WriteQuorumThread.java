package org.example;

import java.io.IOException;

public class WriteQuorumThread extends Thread{

    String key;
    String value;
    int writeQuorum;
    int replicaPosition;
    public WriteQuorumThread(int replicaPosition, String key, String value) {
        this.key = key;
        this.value = value;
        this.replicaPosition= replicaPosition;
    }


    @Override
    public void run() {
        try {
            //77
            Server.sendToPort(replicaPosition, "*set("+key+","+value+")",false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ;
        System.out.println("WriteQuorumThread finished and was done by Thread number "+Thread.currentThread().getId());
    }
}
