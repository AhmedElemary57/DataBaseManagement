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
    public static void mergeCompaction() throws IOException {
        int tempID=1,size=0,counter;
        boolean i=false,j=false;
        if(nextSegmentID%2==1)counter=nextSegmentID-1;
        else counter=nextSegmentID;
        for(int k=0;k<counter;k+=2){
            String file1name=String.valueOf(k+1)+".txt";
            String file2name=String.valueOf(k+2)+".txt";
            File myObj = new File(file1name);
            File myObj2 = new File(file2name);
            File temp = new File("t"+String.valueOf(tempID)+".txt");
            Scanner myReader = new Scanner(myObj);
            Scanner myReader2 = new Scanner(myObj2);
            FileWriter myWriter = new FileWriter("t"+String.valueOf(tempID)+".txt",true);
            String data=myReader.nextLine(),data2=myReader2.nextLine();

            String key1,key2;
            while (myReader.hasNextLine() && myReader2.hasNextLine()) {
                if(i)
                    data = myReader.nextLine();
                if(j)
                    data2 = myReader2.nextLine();
                key1=data.split(",")[0];
                key2=data2.split(",")[0];
                if(key1.equals(key2)){
                    myWriter.write(data2+'\n');
                    i=true;
                    j=true;
                }
                else if(key1.compareTo(key2)>0){
                    myWriter.write(data2+'\n');
                    i=false;
                    j=true;
                }
                else {
                    myWriter.write(data+'\n');
                    i=true;
                    j=false;
                }
                size++;
                if(size>=maxSize){
                    myWriter.close();
                    tempID++;
                    myWriter = new FileWriter("t"+String.valueOf(tempID)+".txt",true);
                    size=0;
                }
            }
            if(!i) myWriter.write(data+'\n');
            if(!j) myWriter.write(data2+'\n');
            size++;
            if(size>=maxSize){
                myWriter.close();
                tempID++;
                myWriter = new FileWriter("t"+String.valueOf(tempID)+".txt",true);
                size=0;
            }
            while (myReader.hasNextLine()){
                data=myReader.nextLine();
                myWriter.write(data+'\n');
                size++;
                if(size>=maxSize){
                    myWriter.close();
                    tempID++;
                    myWriter = new FileWriter("t"+String.valueOf(tempID)+".txt",true);
                    size=0;
                }
            }
            while (myReader2.hasNextLine()){
                data=myReader2.nextLine();
                myWriter.write(data+'\n');
                size++;
                if(size>=maxSize){
                    myWriter.close();
                    tempID++;
                    myWriter = new FileWriter("t"+String.valueOf(tempID)+".txt",true);
                    size=0;
                }
            }
            myReader.close();
            myReader2.close();
            myWriter.close();
            nextSegmentID-=2;
            segmentIDs.remove(0);
            segmentIDs.remove(0);
            i=false;
            j=false;
            if(size!=0)
                tempID++;
            size=0;
            myObj.delete();
            myObj2.delete();
        }
        segmentIDs.clear();
        System.out.println(tempID);
        for (int k = 1; k < tempID; k++) {
            File file = new File("t"+String.valueOf(k)+".txt");
            File rename = new File(String.valueOf(k)+".txt");
            file.renameTo(rename);
            segmentIDs.add(k);
        }
        segmentIDs.add(tempID);
        if(new File("t"+String.valueOf(tempID)+".txt").exists())new File("t"+String.valueOf(tempID)+".txt").delete();
        if(nextSegmentID==1){
            File file = new File(String.valueOf(counter+1)+".txt");
            File rename = new File(String.valueOf(tempID)+".txt");
            file.renameTo(rename);
        }
        nextSegmentID=tempID;
    }





    //Move folder to another folder with its content
    public static void moveFolder(File source, File destination) throws IOException {
        if (source.isDirectory()) {
            if (!destination.exists()) {
                destination.mkdir();
            }
            String files[] = source.list();
            for (String file : files) {
                File srcFile = new File(source, file);
                File destFile = new File(destination, file);
                moveFolder(srcFile, destFile);
            }
        } else {
            Files.move(Paths.get(source.getPath()), Paths.get(destination.getPath()), StandardCopyOption.REPLACE_EXISTING);
        }
    }
    public static void main(String[] args) throws IOException {
        String replicaName="/home/elemary/Projects/DataBaseManagement/Node_Number"+5001+"/ReplicaOf"+5005+"/Data/";
        String newLocation="/home/elemary/Projects/DataBaseManagement/Node_Number"+5006+"/ReplicaOf"+5005+"/Data";
        File srcFile = new File(replicaName);
        File destFile = new File(newLocation);
        moveFolder(srcFile,destFile);




    }


}