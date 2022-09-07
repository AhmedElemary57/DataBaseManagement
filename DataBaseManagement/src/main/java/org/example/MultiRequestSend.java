package org.example;

import java.io.IOException;

public class MultiRequestSend implements Runnable {
    String key;
    int readQuorum;
    int replicaPosition;
    private volatile String value;
    private volatile String request;
    public String getResponse() {
        return value;
    }
    public MultiRequestSend(int replicaPosition, String request) {
    this.request=request;
    this.replicaPosition= replicaPosition;
    }

    @Override
    public void run() {
        try {
            System.out.println("Request started and was done by Thread number ------------> "+Thread.currentThread().getId());
            String response = Requests.sendToPort(replicaPosition, "*"+request,true);
            value = response;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("ReadQuorumThread finished and was done by Thread number "+Thread.currentThread().getId());
    }

}
