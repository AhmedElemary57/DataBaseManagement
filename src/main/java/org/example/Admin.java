package org.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;


public class Admin {

    public static void main(String[] args) throws IOException {

        ProcessBuilder builder = new ProcessBuilder("/home/elemary/Projects/DataBaseManagement/src/main/java/org/example/test.sh");
        builder.directory(new File(System.getProperty("user.home")));
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.start();
    }
}
