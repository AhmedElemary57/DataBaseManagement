package org.example;

import java.io.IOException;
import java.nio.file.*;

public class Rearrange {
    public static void main(String[] args) throws IOException, InterruptedException {
    }


    public static void start(int myNodeNumber, int newNodeNumber, int replicationFactor) throws IOException, InterruptedException {
        // Todo : identify if i should sent files
        if (myNodeNumber > replicationFactor - 1) {
            return;
        }

        // newNodeNumber 6
        // replication factor + node number
        int targetReplica = newNodeNumber - replicationFactor + myNodeNumber;


        String sourceDir =  System.getProperty("user.dir") + "/Node_Number" + (5000 + myNodeNumber);
        String destDir = System.getProperty("user.dir") + "/Node_Number" + (5000 + newNodeNumber);
        String destSubDir = destDir + "/ReplicaOf" + (5000 + newNodeNumber);
        String sourceSubDir = sourceDir + "/ReplicaOf" + (5000 + targetReplica);
        // Move replica from source to destination
//            WatchThread watchThread = new WatchThread(destDir,StandardWatchEventKinds.ENTRY_DELETE);
//            watchThread.start();
            operation("mv", sourceSubDir, destDir);
           // watchThread.join();
            System.out.println("Moved replica from " + sourceSubDir + " to " + destSubDir);
            Thread.sleep(500);
//            watchThread= new WatchThread(sourceDir,StandardWatchEventKinds.ENTRY_CREATE );
//            // copy replica from destination to source
//            watchThread.start();
            operation("cp -R", destSubDir, sourceDir);
            Thread.sleep(500);
//            watchThread.join();
            System.out.println("Copied replica from " + destSubDir + " to " + sourceSubDir);
            startingIndex++;
        }
    }
    public static void operation(String operation, String from, String to){
        String command = operation + " " + from + " " + to;
        try {
            Runtime.getRuntime().exec(command);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}

