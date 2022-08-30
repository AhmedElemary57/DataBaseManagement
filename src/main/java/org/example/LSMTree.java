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

    public void setReplicaId(Integer replicaId) {
        this.replicaId = replicaId;
    }

    public int getMaxMemeTableSize() {
        return maxMemeTableSize;
    }

    public void setMaxMemeTableSize(int maxMemeTableSize) {
        this.maxMemeTableSize = maxMemeTableSize;
    }

    public int getMemTableSize() {
        return memTableSize;
    }

    public void setMemTableSize(int memTableSize) {
        this.memTableSize = memTableSize;
    }
    public void mergeCompaction() throws IOException {
        String diskReplicaPath= "./Node_Number"+ nodeNumber +"/ReplicaOf"+replicaId+"/data/";
        String path = nextSegmentID+".txt";
        int nextSegmentID=segmentNumber,maxSize=maxSegmentSize;
        int tempID=1,size=0,counter;
        boolean i=false,j=false;
        if(nextSegmentID%2==1)counter=nextSegmentID-1;
        else counter=nextSegmentID;
        for(int k=0;k<counter;k+=2){
            String file1name=diskReplicaPath+String.valueOf(k+1)+".txt";
            String file2name=diskReplicaPath+String.valueOf(k+2)+".txt";
            File myObj = new File(file1name);
            File myObj2 = new File(file2name);
            File temp = new File(diskReplicaPath+"t"+String.valueOf(tempID)+".txt");
            Scanner myReader = new Scanner(myObj);
            Scanner myReader2 = new Scanner(myObj2);
            FileWriter myWriter = new FileWriter(diskReplicaPath+"t"+String.valueOf(tempID)+".txt",true);
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
                    myWriter = new FileWriter(diskReplicaPath+"t"+String.valueOf(tempID)+".txt",true);
                    size=0;
                }
            }
            if(!i) myWriter.write(data+'\n');
            if(!j) myWriter.write(data2+'\n');
            size++;
            if(size>=maxSize){
                myWriter.close();
                tempID++;
                myWriter = new FileWriter(diskReplicaPath+"t"+String.valueOf(tempID)+".txt",true);
                size=0;
            }
            while (myReader.hasNextLine()){
                data=myReader.nextLine();
                myWriter.write(data+'\n');
                size++;
                if(size>=maxSize){
                    myWriter.close();
                    tempID++;
                    myWriter = new FileWriter(diskReplicaPath+"t"+String.valueOf(tempID)+".txt",true);
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
                    myWriter = new FileWriter(diskReplicaPath+"t"+String.valueOf(tempID)+".txt",true);
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
        for (int k = 1; k < tempID; k++) {
            File file = new File(diskReplicaPath+"t"+String.valueOf(k)+".txt");
            File rename = new File(diskReplicaPath+String.valueOf(k)+".txt");
            file.renameTo(rename);
        }
        if(new File(diskReplicaPath+"t"+String.valueOf(tempID)+".txt").exists())new File("t"+String.valueOf(tempID)+".txt").delete();
        if(nextSegmentID==1){
            File file = new File(diskReplicaPath+String.valueOf(counter+1)+".txt");
            File rename = new File(diskReplicaPath+String.valueOf(tempID)+".txt");
            file.renameTo(rename);
        }
    }



/**
 * There are some steps that we should do to mange get requests in our app assuming one replica 'that will be repeated for each replica'
 *
 *     Check our cache row and return to the client with the result if present
 *     If not present check in the mem-table and return it if present
 *     Ask bloom filter if it is present
 *     If not so we will check our SSTable from newer to oldest to get the value.
 * **/
    String getValueOf(String key) throws IOException {
        //check our cache row and return to the client with the result if present
        if (rowCache.containsKey(key)){
            return rowCache.get(key);
        }
        //If not present check in the mem-table and return it if present
        String value = memTable.search(key);
        if (memTable.search(key)!=null){
            return value;
        }else {
            //Ask bloom filter if it is present
            if (bloomFilter.mightContain(key)){
                //If not so we will check our SSTable from newer to oldest to get the value.
                return getValueFromSSTable(key,segmentIDs.size());
            }else {
                return null;
            }

        }

    }
    void put(String key,String value) throws IOException {
        ////func
        ///
        memTable.insert(key,value);
        memTableSize=memTable.size();
        bloomFilter.put(key);
        //invalidate the row cache value if it is there
        if(rowCache.containsKey(key)){
            //assume removing the invalidation is done
            rowCache.remove(key);
        }
        if (memTableSize>=maxMemeTableSize){
            flushToDisk();
            segmentIDs.add(segmentNumber);
            memTableSize=0;
            memTable.clear();
        }
    }
    void commitLogs(String key,String value){
        String diskReplicaPath= "./Node_Number"+ nodeNumber +"/ReplicaOf"+replicaId+"/";
        String path = diskReplicaPath+nextSegmentID+".txt";
        File myObj = new File(path);
        try {
            FileWriter myWriter = new FileWriter(myObj,true);
            myWriter.write(key+","+value+'\n');
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    void flushToDisk() throws IOException {
        //flush to disk
        //write the memtable to disk in a json file
        String diskReplicaPath= "./Node_Number"+ nodeNumber +"/ReplicaOf"+replicaId+"/data/";
        segmentNumber++;
        nextSegmentID=segmentNumber;
        String path = nextSegmentID+".txt";

        //write to disk
        List<Node<String>> nodesOfRedBlackTree = memTable.inOrderTraversal();
        File file = new File("."+File.separator+diskReplicaPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        FileWriter fileWriter = new FileWriter(diskReplicaPath+path);
        for (Node<String> node : nodesOfRedBlackTree) {
            fileWriter.write(node.getKey()+","+node.getValue()+","+'\n');
            versionNumber++;
        }
        fileWriter.close();

    }

    //string binary search
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
        }

        //get the value from the SSTable
        //get the path of the SSTable
        String diskReplicaPath= "./Node_Number"+ nodeNumber +"/ReplicaOf"+replicaId+"/data/";
        String path = String.valueOf(segmentIDs.get(fromSegment-1))+".txt";
        //read the file
        File file = new File(diskReplicaPath+path);
        Scanner myReader = new Scanner(file);
        List<String> lines = new ArrayList<>();
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            lines.add(data);
        }
        myReader.close();
        //search the value in the file
        String value = searchKeyInSegment(key,lines.toArray(new String[lines.size()]));
        if (value!=null) {
            return value;
        }


        return getValueFromSSTable(key,fromSegment-1);
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        LSMTree lsmTree = new LSMTree(5007,788,5,10);
        lsmTree.put("1","a");
        lsmTree.put("2","2");
        lsmTree.put("2","3");
        lsmTree.put("4","4");
        lsmTree.put("5","5");
        lsmTree.put("6","I'm here");
        lsmTree.put("7","7");
        lsmTree.put("8","8");
        lsmTree.put("9","9");
        lsmTree.put("10","10");
        lsmTree.put("11","11");
        lsmTree.put("12","12");
        lsmTree.put("13","13");
        lsmTree.put("14","14");
        lsmTree.put("15","15");
        lsmTree.put("16","16");
        lsmTree.put("17","17");
        lsmTree.put("18","18");
        lsmTree.put("19","19");
        lsmTree.put("20","20");
        lsmTree.put("21","21");
        lsmTree.put("22","22");
        lsmTree.put("23","23");
        lsmTree.put("24","24");
        lsmTree.put("25","25");
        lsmTree.put("26","26");
        lsmTree.put("27","27");
        lsmTree.put("28","28");
        lsmTree.put("29","29");
        lsmTree.put("30","30");
        lsmTree.put("31","31");
        lsmTree.put("32","32");
        lsmTree.put("33","33");
        lsmTree.put("34","34");
        lsmTree.put("35","35");
        lsmTree.put("36","36");
        lsmTree.put("37","37");
        lsmTree.put("38","38");
        lsmTree.put("39","39");
        lsmTree.put("40","40");
        System.out.println(lsmTree.getValueFromSSTable("2", lsmTree.segmentIDs.size()));
        lsmTree.mergeCompaction();
        lsmTree.put("1","a");
        lsmTree.put("2","2");
        lsmTree.put("2","3");
        lsmTree.put("4","4");
        lsmTree.put("5","5");
        lsmTree.put("6","I'm here");
        lsmTree.put("7","7");
        lsmTree.put("8","8");
        lsmTree.put("9","9");
        lsmTree.put("10","10");
        lsmTree.put("11","11");
        lsmTree.put("12","12");
        lsmTree.put("13","13");
        lsmTree.put("14","14");
        lsmTree.put("15","15");
        lsmTree.put("16","16");
        lsmTree.put("17","17");
        lsmTree.put("18","18");
        lsmTree.put("19","19");
        lsmTree.mergeCompaction();


    }

}
