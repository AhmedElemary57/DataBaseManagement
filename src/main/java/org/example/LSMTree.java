package org.example;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.util.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

public class LSMTree {
    Integer nodeNumber,replicaId,nextSegmentID;
    int maxMemeTableSize, memTableSize,segmentNumber,versionNumber,maxSegmentSize;
    List<Integer> segmentIDs;
    RedBlackTree<String> memTable;
    Map<String,String> rowCache;
    BloomFilter<String> bloomFilter ;
    String diskPath="";
    public LSMTree(Integer serverName, Integer memTableID, int maxMemeTableSize, int maxSegmentSize) {
        this.nodeNumber = serverName;
        this.replicaId = memTableID;
        this.maxMemeTableSize = maxMemeTableSize;
        this.memTable = new RedBlackTree<>();
        this.segmentNumber=0;
        this.maxSegmentSize=maxSegmentSize;
        this.nextSegmentID=segmentNumber;
        this.rowCache = new HashMap<>();
        this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_16), 100, 0.01);
        segmentIDs= new ArrayList<>();
        this.versionNumber=0;
        this.rowCache= new HashMap<>();
        this.diskPath= "/home/elemary/Projects/DataBaseManagement/Node_Number"+ nodeNumber +"/ReplicaOf"+replicaId+"/";
    }
    public Integer getNodeNumber() {
        return nodeNumber;
    }

    public void setNodeNumber(Integer nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public Integer getReplicaId() {
        return replicaId;
    }

    public void mergeCompaction() throws IOException {
        diskPath=  "/home/elemary/Projects/DataBaseManagement/Node_Number"+ nodeNumber +"/ReplicaOf"+replicaId+"/";
        int nextSegmentID=segmentNumber,maxSize=maxSegmentSize;
        int tempID=1,size=0,counter;
        boolean i=false,j=false;
        if(nextSegmentID%2==1)counter=nextSegmentID-1;
        else counter=nextSegmentID;
        for(int k=0;k<counter;k+=2){
            String file1name=diskPath+String.valueOf(k+1)+".txt";
            String file2name=diskPath+String.valueOf(k+2)+".txt";
            File myObj = new File(file1name);
            File myObj2 = new File(file2name);
            File temp = new File(diskPath+"t"+String.valueOf(tempID)+".txt");
            Scanner myReader = new Scanner(myObj);
            Scanner myReader2 = new Scanner(myObj2);
            FileWriter myWriter = new FileWriter(diskPath+"t"+String.valueOf(tempID)+".txt",true);
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
                    myWriter = new FileWriter(diskPath+"t"+String.valueOf(tempID)+".txt",true);
                    size=0;
                }
            }
            if(!i) myWriter.write(data+'\n');
            if(!j) myWriter.write(data2+'\n');
            size++;
            if(size>=maxSize){
                myWriter.close();
                tempID++;
                myWriter = new FileWriter(diskPath+"t"+String.valueOf(tempID)+".txt",true);
                size=0;
            }
            while (myReader.hasNextLine()){
                data=myReader.nextLine();
                myWriter.write(data+'\n');
                size++;
                if(size>=maxSize){
                    myWriter.close();
                    tempID++;
                    myWriter = new FileWriter(diskPath+"t"+String.valueOf(tempID)+".txt",true);
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
                    myWriter = new FileWriter(diskPath+"t"+String.valueOf(tempID)+".txt",true);
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
            File file = new File(diskPath+"t"+String.valueOf(k)+".txt");
            File rename = new File(diskPath+String.valueOf(k)+".txt");
            file.renameTo(rename);
            segmentIDs.add(k);
        }
        segmentIDs.add(tempID);
        if(new File(diskPath+"t"+String.valueOf(tempID)+".txt").exists())
            new File(diskPath+"t"+String.valueOf(tempID)+".txt").delete();
        if(nextSegmentID==1){
            File file = new File(diskPath+String.valueOf(counter+1)+".txt");
            File rename = new File(diskPath+String.valueOf(tempID)+".txt");
            file.renameTo(rename);
        }
        segmentNumber=tempID;
    }



/**
 * There are some steps that we should do to mange get requests in our app assuming one replica 'that will be repeated for each replica'
 *
 *     Check our cache row and return to the client with the result if present
 *     If not present check in the mem-table and return it if present
 *     Ask bloom filter if it is present
 *     If not so we will check our SSTable from newer to oldest to get the value.
 * **/
public String getValueOf(String key) throws IOException {
        if (rowCache.containsKey(key)){
            return rowCache.get(key);
        }
        String value = memTable.search(key);
        if (memTable.search(key)!=null){
            return value;
        } else {
            if (bloomFilter.mightContain(key)){
                return getValueFromSSTable(key,segmentIDs.size());
            } else {
                return null;
            }
        }
    }
    public void setValueOf(String key, String value) throws IOException {
        memTable.insert(key,value);
        memTableSize = memTable.size();
        commitLogs(key,value);
        bloomFilter.put(key);
        if(rowCache.containsKey(key)){
            rowCache.remove(key);
        }
        if (memTableSize>=maxMemeTableSize){
            flushToDisk();
            segmentIDs.add(segmentNumber);
            memTableSize=0;
            memTable.clear();
        }
    }
    void commitLogs(String key,String value) throws IOException {
        String path = diskPath+"commitLog"+nodeNumber+".txt";
        File file = new File(diskPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        FileWriter fileWriter = new FileWriter(path,true);
        try {
            fileWriter.write(key+","+value+" "+System.currentTimeMillis()+'\n');
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void flushToDisk() throws IOException {
        segmentNumber++;
        nextSegmentID=segmentNumber;
        String fileName = nextSegmentID+".txt";
        List<Node<String>> nodesOfRedBlackTree = memTable.inOrderTraversal();
        File file = new File(diskPath+"/Data/");
        if (!file.exists()) {
            file.mkdirs();
        }
        FileWriter fileWriter = new FileWriter(diskPath+"/Data/"+fileName);
        for (Node<String> node : nodesOfRedBlackTree) {
            fileWriter.write(node.getKey()+","+node.getValue()+'\n');
            versionNumber++;
        }
        String path = diskPath+"commitLog"+nodeNumber+".txt";
        File commitFile = new File(path);
        commitFile.delete();
        fileWriter.close();
    }
    String searchKeyInSegment(String key,String[] segmentData) throws IOException {
        int low = 0;
        int high = segmentData.length-1;
        while (low <= high) {
            int mid = (low + high) / 2;
            String[] keyValue = segmentData[mid].split(",");
            if (keyValue[0].compareTo(key) < 0) {
                low = mid + 1;
            } else if (keyValue[0].compareTo(key) > 0) {
                high = mid - 1;
            } else {
                return keyValue[1];
            }
        }
        return null;
    }
    String getValueFromSSTable(String key,int fromSegment) throws IOException {
        if (fromSegment==0){
            return null;
        }String segmentName = segmentIDs.get(fromSegment - 1) +".txt";
        File file = new File(diskPath+"/Data/"+segmentName);
        Scanner myReader = new Scanner(file);
        List<String> lines = new ArrayList<>();
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            lines.add(data);
        }
        myReader.close();
        String value = searchKeyInSegment(key,lines.toArray(new String[0]));
        if (value!=null) {
            return value;
        }
        return getValueFromSSTable(key,fromSegment-1);
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        LSMTree lsmTree = new LSMTree(5007,788,5,10);
        lsmTree.setValueOf("1","a");
        lsmTree.setValueOf("2","2");
        lsmTree.setValueOf("2","3");
        lsmTree.setValueOf("4","4");
        lsmTree.setValueOf("5","5");
        lsmTree.setValueOf("6","I'm here");
        lsmTree.setValueOf("7","7");
        lsmTree.setValueOf("8","8");
        lsmTree.setValueOf("9","9");
        lsmTree.setValueOf("10","10");
        lsmTree.setValueOf("1","11");
        lsmTree.setValueOf("12","12");
        lsmTree.setValueOf("13","13");
        lsmTree.setValueOf("14","14");
        lsmTree.setValueOf("15","15");
        lsmTree.setValueOf("16","16");
        lsmTree.setValueOf("17","17");
        lsmTree.setValueOf("18","18");
        lsmTree.setValueOf("19","19");
        lsmTree.setValueOf("20","20");
        lsmTree.setValueOf("21","21");
        lsmTree.setValueOf("22","22");
        lsmTree.setValueOf("23","23");
        lsmTree.setValueOf("24","24");
        lsmTree.setValueOf("25","25");
        lsmTree.setValueOf("26","26");
        lsmTree.setValueOf("27","27");
        lsmTree.setValueOf("28","28");
        lsmTree.setValueOf("29","29");
        lsmTree.setValueOf("30","30");
        lsmTree.setValueOf("31","31");
        lsmTree.setValueOf("32","32");
        lsmTree.setValueOf("33","33");
        lsmTree.setValueOf("34","34");
        lsmTree.setValueOf("35","35");
        lsmTree.setValueOf("36","36");
        lsmTree.setValueOf("37","37");
        lsmTree.setValueOf("38","38");
        lsmTree.setValueOf("39","39");
        lsmTree.setValueOf("40","40");

    }

}
