package org.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;


public class Admin {
    public static void startServers(List<Integer> data) throws IOException {
        // write data into test.sh file
        try {
            FileWriter myWriter = new FileWriter("/home/elemary/Projects/DataBaseManagement/src/main/java/org/example/test.sh");
            myWriter.write("#!/bin/bash\n" +
                    "for i in {1.." + data.get(0) + "}\n" +
                    "do\n" +
                    "   gnome-terminal -- bash -c \"java -jar /home/elemary/Projects/DataBaseManagement/out/artifacts/DataBaseManagement_jar/DataBaseManagement.jar $i "
                    + data.get(0) + " " + data.get(1) + " " + data.get(2) + " " + data.get(3) +
                    " " + data.get(4) + " " + data.get(5) + " " + data.get(6) + "; exec bash\"\n" +
                    "done\n");
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        ProcessBuilder pb = new ProcessBuilder("Projects/DataBaseManagement/src/main/java/org/example/test.sh");
        pb.directory(new File(System.getProperty("user.home")));
        pb.start();
    }

}