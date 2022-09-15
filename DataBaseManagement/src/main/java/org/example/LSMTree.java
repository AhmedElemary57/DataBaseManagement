package org.example;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

public class LSMTree {
    Integer nodeNumber,replicaId;
    int maxMemeTableSize;
    int memTableSize;
    static int maxSegmentSize;
    List<Integer> segmentIDs;
    RedBlackTree<String> memTable;
    Map<String,String> rowCache;
    BloomFilter<String> bloomFilter ;
    String diskPath="";
    boolean withCrashRecovery;
    public LSMTree(Integer serverName, Integer memTableID, int maxMemeTableSize, int maxSegmentSize, boolean withCrashRecovery) {
        this.nodeNumber = serverName;
        this.replicaId = memTableID;
        this.maxMemeTableSize = maxMemeTableSize;
        this.memTable = new RedBlackTree<>();
        this.diskPath= "" + Server.Path+"/Node_Number"+ nodeNumber +"/ReplicaOf"+replicaId+"/";
        this.maxSegmentSize=maxSegmentSize;
        this.rowCache = new HashMap<>();
        this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 100, 0.01);
        this.segmentIDs= new ArrayList<>();
        this.rowCache= new HashMap<>();
        this.withCrashRecovery=withCrashRecovery;
    }
    public Integer getNodeNumber() {
        return nodeNumber;
    }
    public void fillSegmentIDs(){
        segmentIDs=new ArrayList<>();
        System.out.println("filling segment ids" + segmentIDs );
        diskPath= "" + Server.Path+"/Node_Number"+ nodeNumber +"/ReplicaOf"+replicaId+"/";
        System.out.println("disk path : " + diskPath);
        File folder = new File(diskPath+"Data/");
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles != null) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    segmentIDs.add(Integer.valueOf(listOfFiles[i].getName().split("\\.")[0]));
                }
            }
        }
        System.out.println("filled segment ids" + segmentIDs);

    }
    public void setNodeNumber(Integer nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public Integer getReplicaId() {
        return replicaId;
    }

    public static int numberOfRecords(File file){
        int count=0;
        try {
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                count++;
                sc.nextLine();
            }
            sc.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return count;
    }

    public void setReplicaId(Integer replicaId) {
        this.replicaId = replicaId;
    }

    public void mergeFiles(File file1, File file2) throws IOException {
        PrintWriter pw = new PrintWriter(diskPath+"Data/temp.txt");
        BufferedReader br1 = new BufferedReader(new FileReader(file1.toPath().toString()));
        BufferedReader br2 = new BufferedReader(new FileReader(file2.toPath().toString()));
        String line1 = br1.readLine();
        String line2 = br2.readLine();

        while (line1 != null && line2 != null) {

            String[] data1 = line1.split(",");
            String[] data2 = line2.split(",");
            String key1 = data1[0];
            String key2 = data2[0];

            if (key1.compareTo(key2) > 0) {
                pw.println(line2);
                line2 = br2.readLine();
            } else  {
                pw.println(line1 );
                line1 = br1.readLine();

            }
        }

        while (line1!= null) {
            pw.println( line1 );
            line1 = br1.readLine();
        }
        while (line2!= null) {
            pw.println( line2 );
            line2 = br2.readLine();
        }
        pw.flush();
        pw.close();
        br1.close();
        br2.close();
        String mergedSegmentName = file2.getName();
        file1.delete();
        file2.delete();
        File newFile = new File(diskPath+"Data/"+mergedSegmentName);
        File oldFile = new File(diskPath+"Data/temp.txt");
        oldFile.renameTo(newFile);

    }
    // get pathes of two files and merge them in new one
    public void mergeTwoSegments(String newerFilePath, String olderFilePath) throws IOException {
        File file1 = new File(newerFilePath);
        File file2 = new File(olderFilePath);
        Collections.sort(segmentIDs);
        if (numberOfRecords(file1) + numberOfRecords(file2) <= maxSegmentSize) {
            mergeFiles(file1, file2);
            this.segmentIDs.remove(Integer.valueOf(file1.getName().split("\\.")[0]));
        }
    }
    public void mergeCompaction() throws IOException {
        Collections.sort(segmentIDs);
        for (int i = segmentIDs.size()-1; i > 0; i--) {
            mergeTwoSegments(diskPath+"Data/"+segmentIDs.get(i) + ".txt", diskPath+"Data/"+segmentIDs.get(i - 1) + ".txt");
        }
    }
    public String getValueOf(String key) throws IOException {
        // print row cache
        System.out.println("row cache");
        for (Map.Entry<String, String> entry : rowCache.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
        if (rowCache.containsKey(key)){
            System.out.println("------------------------------ Cache Hit ----------------------------");
            return rowCache.get(key);
        }
        String value = memTable.search(key);
        if (memTable.search(key)!=null){
            return value;
        } else {
            if (bloomFilter.mightContain(key) || withCrashRecovery){
                value = getValueFromSSTable(key,segmentIDs.size());
                rowCache.put(key, value);
                return value;
            } else {
                System.out.println("Bloom Filter Missed value of key: " + key);
                return null;
            }
        }
    }
    public void setValueOf(String key, String value) throws IOException {
        memTable.insert(key,value);
        memTableSize = memTable.size();
        if(withCrashRecovery){
        commitLogs(key,value);
        }
        else {
        bloomFilter.put(key);
        }

        if(rowCache.containsKey(key)){
            rowCache.remove(key);
        }
        if (memTableSize>=maxMemeTableSize){
            flushToDisk();
            memTableSize=0;
            memTable.clear();
        }
    }
    void commitLogs(String key,String value) throws IOException {
        String path = diskPath+"commitLog"+replicaId+".txt";
        File file = new File(diskPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        FileWriter fileWriter = new FileWriter(path,true);
        try {
            fileWriter.write(key+","+value+"\n");
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    void flushToDisk() throws IOException {
        int max;
        if (segmentIDs.size()==0){
            max=0;
        }
        else {
           max = Collections.max(segmentIDs);
        }

        String fileName = max+1+".txt";
        List<Node<String>> nodesOfRedBlackTree = memTable.inOrderTraversal();
        File file = new File(diskPath+"Data/");
        if (!file.exists()) {
            file.mkdirs();
        }
        FileWriter fileWriter = new FileWriter(diskPath+"Data/"+fileName);
        for (Node<String> node : nodesOfRedBlackTree) {
            fileWriter.write(node.getKey()+","+node.getValue()+'\n');
        }
        segmentIDs.add(max+1);
        String path = diskPath+"commitLog"+replicaId+".txt";
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
        File file = new File(diskPath+"Data/"+segmentName);
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
    public void startCompaction() throws IOException, InterruptedException{
        System.out.println("---- Compaction Started for "+replicaId+"---");
        CompactionThread compactionThread = new CompactionThread(this);
        compactionThread.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        LSMTree lsmTree = new LSMTree(5017,5017,5,10, true);

        for (int i=0; i<19; i++){
            lsmTree.setValueOf("key"+i,"value"+i);
        }
        lsmTree.setValueOf("key"+20,"value"+200);
        lsmTree.setValueOf("key"+21,"value"+21);

        lsmTree.startCompaction();

        for (int i=20; i<30; i++){
            lsmTree.setValueOf("key"+i,"value"+i);
        }

    }

}
