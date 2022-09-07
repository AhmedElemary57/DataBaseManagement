package org.example;

import java.io.IOException;
import java.nio.file.*;

public class Rearrange {
    public static void main(String[] args) throws IOException, InterruptedException {
        start(6,3,"/home/elemary/Projects/DataBaseManagement/");
    }


    public static void start(int nodeNumber, int replicationFactor, String directory) throws IOException, InterruptedException {
        int startingIndex = nodeNumber - replicationFactor + 1;
        for (int i = 1; i <= replicationFactor - 1; i++) {
            String sourceDir = directory + "Node_Number" + (5000 + i);
            String destDir = directory + "Node_Number" + (5000 + nodeNumber);
            String destSubDir = destDir + "/ReplicaOf" + (5000 + nodeNumber);
            String sourceSubDir = sourceDir + "/ReplicaOf" + (5000 + startingIndex);
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
        String command = operation+" " +from+" "+to;
        try {
            Runtime.getRuntime().exec(command);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}

