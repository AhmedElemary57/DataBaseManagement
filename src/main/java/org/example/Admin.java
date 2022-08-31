package org.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;


public class Admin {

    public static void main(String[] args) throws IOException {

        ProcessBuilder pb = new ProcessBuilder("/home/elemary/Projects/DataBaseManagement/src/main/java/org/example/test.sh");
        pb.directory(new File(System.getProperty("user.home")));
        pb.start();
    }
}