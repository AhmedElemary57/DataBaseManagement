package org.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Main {
    static int nextSegmentID=2,maxSize=10;
    static List<Integer> segmentIDs=new LinkedList<>();
    public static void mergeCompaction() throws IOException {
        int tempID=1,size=0;
        boolean i=false,j=false;
        File myObj = new File("1.txt");
        File myObj2 = new File("2.txt");
        File temp = new File("t1.txt");
        Scanner myReader = new Scanner(myObj);
        Scanner myReader2 = new Scanner(myObj2);
        FileWriter myWriter = new FileWriter("t1.txt",true);
        String data=myReader.nextLine(),data2=myReader2.nextLine();
        int key1,key2;
        while (myReader.hasNextLine() && myReader2.hasNextLine()) {
            if(i)
                data = myReader.nextLine();
            if(j)
                data2 = myReader2.nextLine();
            key1=Integer.parseInt(data.split(",")[0]);
            key2=Integer.parseInt(data2.split(",")[0]);
            if(key1==key2){
                myWriter.write(data2+'\n');
                i=true;
                j=true;
            }
            else if(key1>key2){
                myWriter.write(data2+'\n');
                i=false;
                j=true;
            }
            else {
                myWriter.write(data+'\n');
                i=true;
                j=false;
            }
        }
        if(!i) myWriter.write(data+'\n');
        if(!j) myWriter.write(data2+'\n');

        while (myReader.hasNextLine()){
            data=myReader.nextLine();
            myWriter.write(data+'\n');
        }
        while (myReader2.hasNextLine()){
            data=myReader2.nextLine();
            myWriter.write(data+'\n');
        }
        myReader.close();
        myReader2.close();
        myWriter.close();

    }
    public static void main(String[] args) throws IOException {
        for (int i = 1; i <= nextSegmentID; i++) {
            segmentIDs.add(i);
        }
        mergeCompaction();


    }


}