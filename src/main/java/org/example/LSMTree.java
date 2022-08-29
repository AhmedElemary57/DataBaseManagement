package org.example;

import org.json.JSONObject;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;
public class LSMTree {
    String serverName,memTableID,nextSegmentID;
    int maxMemeTableSize, memTableSize,segmentNumber;
    RedBlackTree<String> memTable;

    public LSMTree(String serverName, String memTableID, int maxMemeTableSize) {
        this.serverName = serverName;
        this.memTableID = memTableID;
        this.maxMemeTableSize = maxMemeTableSize;
        this.memTable = new RedBlackTree<>();
        this.segmentNumber=1;
         this.nextSegmentID=String.valueOf(segmentNumber);
         this.rowCache= new HashMap<>();


    }
    Map<String,String> rowCache;

    String getValueOf(String key){
        String value = memTable.search(key);
        if (memTable.search(key)!=null){
            return value;
        }else {
            return "";
        }
    }
    void put(String key,String value) throws IOException {
        commitLogs(key, value);
        memTable.insert(key,value);
        memTableSize++;
        //invalidate the row cache value if it is there
        if(rowCache.containsKey(key)){
            //assume removing the invalidation is done
            rowCache.remove(key);
        }
        if (memTableSize>=maxMemeTableSize){
            flushToDisk();
            memTableSize=0;
            memTable.clear();
        }
    }
    void commitLogs(String key,String value){
        File myObj = new File(serverName+".txt");
        try {
            FileWriter myWriter = new FileWriter(serverName+".txt",true);
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
        String diskReplicaPath= "./Node_Number"+serverName+"/ReplicaOf"+memTableID+"/data/";
        String path = nextSegmentID+".json";

        //write to disk
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("segmentID",nextSegmentID);
        List<Node<String>> nodesOfRedBlackTree = memTable.inOrderTraversal();
        List<Map<String,String>> listOfData = new ArrayList<>();
        for (Node<String> node:nodesOfRedBlackTree){
            Map<String,String> data = new HashMap<>();
            data.put("key",node.getKey());
            data.put("value",node.getValue());
            listOfData.add(data);
        }
        jsonObject.put("data",listOfData);

        File file = new File("."+File.separator+diskReplicaPath);
        if (!file.exists()) {
            file.mkdirs();
        }

        FileWriter fileWriter = new FileWriter(diskReplicaPath+path);
        PrintWriter writer = new PrintWriter(fileWriter);
        writer.write(jsonObject.toString());
        fileWriter.flush();

        segmentNumber++;
        nextSegmentID=String.valueOf(segmentNumber);
    }

    public static void main(String[] args) throws IOException {
        LSMTree lsmTree = new LSMTree("5005","5708",5);
        lsmTree.put("1","1");
        lsmTree.put("2","2");
        lsmTree.put("3","3");
        lsmTree.put("4","4");
        lsmTree.put("5","5");
        lsmTree.put("6","6");
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


    }

}
