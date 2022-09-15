package org.example;

import java.io.*;
import java.util.List;


public class Admin {

    public static void main(String[] args) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader("test.sh"))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }

    }

    private static String userPath = "./out/artifacts/DataBaseManagement_jar/DataBaseManagement.jar";
    public static int count = 0;
    public static void startServers(List<Integer> data) throws IOException {
        // write data into test.sh file
        try {
            FileWriter myWriter = new FileWriter("test.sh");
            myWriter.write("#!/bin/bash\n" +
                    "for i in {1.." + data.get(0) + "}\n" +
                    "do\n" +
                    "   gnome-terminal -- bash -c \"java -jar " + userPath + " $i "
                    + data.get(0) + " " + data.get(1) + " " + data.get(2) + " " + data.get(3) +
                    " " + data.get(4) + " " + data.get(5) + " " + data.get(6) +" 1"+" 0"+"; exec bash\"\n" +
                    "done\n");
            myWriter.close();
            count = data.get(0);
            Requests.sendToPort(7999, "start " + data.get(0), false);
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        ProcessBuilder pb = new ProcessBuilder( "./test.sh");
        pb.start();
    }
    public static void addNode(List<Integer> data) throws IOException {
        // write data into test.sh file
        try {
            FileWriter myWriter = new FileWriter("test.sh");
            count++;
            myWriter.write("#!/bin/bash\n" +
                    "gnome-terminal -- bash -c \"java -jar " + userPath + " "
                    + count + " "
                    + count + " " + data.get(1) + " " + data.get(2) + " " + data.get(3) +
                    " " + data.get(4) + " " + data.get(5) + " " + data.get(6) + " 1"+" 1"+"; exec bash\"\n" );
            myWriter.close();

            for (int i = 1; i < count ; i++) {
                Requests.sendToPort(5000+i,"addNode "+(5000+count), false);
            }
            Requests.sendToPort(5000,"addNode "+ (5000 + count), false);

        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        System.out.println(count);
        ProcessBuilder pb = new ProcessBuilder("./test.sh");
        pb.start();
    }

}