package org.example;

import org.apache.commons.codec.digest.MurmurHash3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Main {
    static int nextSegmentID=6,maxSize=10;
    static List<Integer> segmentIDs=new LinkedList<>();

    // function to merge two files in new one




    public static void main(String[] args) throws IOException {
        String replicaName="/home/elemary/Projects/DataBaseManagement/Node_Number"+5001+"/ReplicaOf"+5005+"/Data/";
        String newLocation="/home/elemary/Projects/DataBaseManagement/Node_Number"+5006+"/ReplicaOf"+5005+"/Data";
        File srcFile = new File(replicaName);
        File destFile = new File(newLocation);


    }


}