package org.example;

import com.amazonaws.services.s3.internal.ObjectExpirationHeaderHandler;
import org.example.RingStructure;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.*;
import java.util.Scanner;

public class Rehash {
    static ArrayList<ArrayList<String>> ranges;
    public static int  findIndex(List<String> list, String k) {
        // Lower and upper bounds
        int start = 0;
        int end = list.size() - 1;
        // Traverse the search space
        while (start <= end) {
            int mid = (start + end) / 2;
            // If K is found
            if (list.get(mid).equals(k))
                return mid;
            else if (list.get(mid).compareTo(k) < 0)
                start = mid + 1;
            else
                end = mid - 1;
        }

        // Return insert position
        return end;
    }

    public static void deleteOldSegment(ArrayList<ArrayList<String>> ranges) {

    }
    /**
     * @param ranges : List of pairs represents the ranges (start, end).
     * @param newNodePath : The path of the ssTable file for the new node.
     * @param segmentPath : The path of the old ssTable file to transfer the data from it.
     *
     */
    public static void createNewSegment(ArrayList<ArrayList<String>> ranges, String newNodePath, String newSegmentName, String segmentPath) throws IOException {

        File oldFile = new File(segmentPath);
        File newFile = new File(newNodePath);
        if (!newFile.exists()) {
            newFile.mkdirs();
        }

        Scanner myReader = new Scanner(oldFile);
        List<String> keys=new ArrayList<>();
        List<String> values= new ArrayList<>();
        while (myReader.hasNextLine()){
            String line = myReader.nextLine();
            String[] keyValue = line.split(",");
            keys.add(keyValue[0]);
            values.add(keyValue[1]);
        }
        myReader.close();

        FileWriter newFileWriter = new FileWriter(newFile+"/"+newSegmentName+".txt", true);
        for (int i = 0; i < ranges.size(); i++) {
            String start = ranges.get(i).get(0);
            String end = ranges.get(i).get(1);
            int startInx = findIndex(keys, start);
            int endInx = findIndex(keys, end);
            for (int j = startInx; j <= endInx; j++) {
                newFileWriter.write(keys.get(j) + "," + values.get(i) + "\n");
            }
        }
    }
    public static void createNewPartition(String newNodePath, String oldNewPath) throws IOException {
        File folder = new File(oldNewPath);
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles != null) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    createNewSegment(ranges,newNodePath,listOfFiles[i].getName().split("\\.")[0],listOfFiles[i].getPath());
                }
            }
        }
    }
    public static void main(String[] args) {
        ranges = new ArrayList<>();
        ranges.add(new ArrayList<>(Arrays.asList("key15", "key19")));
        ranges.add(new ArrayList<>(Arrays.asList("key30", "key35")));
        ranges.add(new ArrayList<>(Arrays.asList("key80", "key85")));

        String oldNodePath = "/home/elemary/Projects/DataBaseManagement/Node_Number"+ 3 +"/ReplicaOf"+3+"/Data/";
        String newNodePath = "/home/elemary/Projects/DataBaseManagement/Node_Number"+ 5017 +"/ReplicaOf"+5017+"/Data/";
        try {
            createNewPartition(newNodePath,oldNodePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
