package org.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;


public class Admin {
    private static String[] mohamed = new String[]{"/home/al-sharif-mohamed/IdeaProjects/DataBaseManagement/src/main/java/org/example/test.sh",
            "/home/al-sharif-mohamed/IdeaProjects/DataBaseManagement/out/artifacts/DataBaseManagement_jar/DataBaseManagement.jar",
            "IdeaProjects/DataBaseManagement/src/main/java/org/example/test.sh"};
    private static String[] elemary = new String[] {"/home/elemary/Projects/DataBaseManagement/src/main/java/org/example/test.sh",
            "/home/elemary/Projects/DataBaseManagement/out/artifacts/DataBaseManagement_jar/DataBaseManagement.jar",
            "Projects/DataBaseManagement/src/main/java/org/example/test.sh"
    };
    public static int count = 0;
    public static void startServers(List<Integer> data) throws IOException {
        // write data into test.sh file
        try {
            FileWriter myWriter = new FileWriter(mohamed[0]);
            myWriter.write("#!/bin/bash\n" +
                    "for i in {1.." + data.get(0) + "}\n" +
                    "do\n" +
                    "   gnome-terminal -- bash -c \"java -jar " + mohamed[1] + " $i "
                    + data.get(0) + " " + data.get(1) + " " + data.get(2) + " " + data.get(3) +
                    " " + data.get(4) + " " + data.get(5) + " " + data.get(6) + "; exec bash\"\n" +
                    "done\n");
            myWriter.close();
            count = 6;
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        ProcessBuilder pb = new ProcessBuilder(mohamed[2]);
        pb.directory(new File(System.getProperty("user.home")));
        pb.start();
    }
    public static void addNode(List<Integer> data) throws IOException {
        // write data into test.sh file
        try {
            FileWriter myWriter = new FileWriter(mohamed[0]);
            myWriter.write("#!/bin/bash\n" +
                    "gnome-terminal -- bash -c \"java -jar " + mohamed[1] + " "
                    + count + " "
                    + count + " " + data.get(1) + " " + data.get(2) + " " + data.get(3) +
                    " " + data.get(4) + " " + data.get(5) + " " + data.get(6) + "; exec bash\"\n" );
            myWriter.close();

            for (int i = 1; i < count ; i++) {
                Server.sendToPort(5000+i,"addNode "+(5000+count), false);
            }
            count++;
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        System.out.println(count);
        ProcessBuilder pb = new ProcessBuilder(mohamed[2]);
        pb.directory(new File(System.getProperty("user.home")));
        pb.start();
    }

}