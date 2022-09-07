package org.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Main {
    static int nextSegmentID=6,maxSize=10;
    static List<Integer> segmentIDs=new LinkedList<>();

    public static void moveReplicas(String from,String to){
        String command = "mv "+from+" "+to;
        try {
            Runtime.getRuntime().exec(command);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {

    }


}