package org.example;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.BufferedReader;
import java.util.*;
import java.util.function.Predicate;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;

public class LSMTree {
    String serverName,memTableID,nextSegmentID;
    int maxMemeTableSize, memTableSize,segmentNumber,versionNumber;
    List<Integer> segmentIDs;
    RedBlackTree<String> memTable;
    Map<String,String> rowCache;
    BloomFilter<String> bloomFilter ;
    public LSMTree(String serverName, String memTableID, int maxMemeTableSize) {
        this.serverName = serverName;
        this.memTableID = memTableID;
        this.maxMemeTableSize = maxMemeTableSize;
        this.memTable = new RedBlackTree<>();
        this.segmentNumber=0;
        this.nextSegmentID=String.valueOf(segmentNumber);
        this.rowCache = new HashMap<>();
        this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_16), 100, 0.01);
        segmentIDs= new ArrayList<>();
        this.versionNumber=0;
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

    void flushToDisk() throws IOException {
        //flush to disk
        //write the memtable to disk in a json file
        String diskReplicaPath= "./Node_Number"+serverName+"/ReplicaOf"+memTableID+"/data/";
        segmentNumber++;
        nextSegmentID=String.valueOf(segmentNumber);
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
        String diskReplicaPath= "./Node_Number"+serverName+"/ReplicaOf"+memTableID+"/data/";
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
    public static void main(String[] args) throws IOException {
        LSMTree lsmTree = new LSMTree("5075","4785",5);
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


    }

}
