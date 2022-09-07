package org.example;

import org.apache.commons.codec.digest.MurmurHash3;

import java.net.*;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.Semaphore;

public class Server {
    static Semaphore sem = new Semaphore(1);
    static RingStructure ringStructure;
    static String currentValue = "Error 404 Not Found";
    static int currentPortNumber;
    static int replicationFactor;
    static List<LSMTree> lsmTrees;
    static int numberOfNodes;
    static int nodeNumber;
    static int numberOfVirtualNodes;
    static int maxSegmentSize;
    static int maxMemTableSize;
    static NodesReplicasMapping nodesReplicasMapping;


    static boolean hasReplica(int partitionID) {
        return nodesReplicasMapping.whichReplicasBelongToNode.get(currentPortNumber).contains(partitionID);
    }

    static List<String> sendRequestToAllReplicas(int partitionID, String request) throws IOException, InterruptedException {
        List<Integer> replicasPosition = nodesReplicasMapping.getPositionReplicasOfNode(partitionID);
        System.out.println("replicasPosition = " + replicasPosition);
        List<String> responses = new ArrayList<>();

        if (replicasPosition.contains(Integer.valueOf(currentPortNumber))){
            String key = "";
            String myResponse = "Set Successful";
            if (request.startsWith("get")){
                key = request.substring(4,request.length()-1);
                myResponse = findValueFromPartitions(neededReplicaId ,key );
            }
            replicasPosition.remove(Integer.valueOf(currentPortNumber));
            responses.add(myResponse);
        }
        List<MultiRequestSend> multiRequestSends = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < replicasPosition.size(); i++) {
            multiRequestSends.add(new MultiRequestSend(replicasPosition.get(i), request));
            threads.add(new Thread(multiRequestSends.get(i)));
            threads.get(i).start();
        }
        System.out.println("Thread joined waiting for response");
        for (Thread thread : threads) {
            thread.join();
        }
        // All threads ended
        for (MultiRequestSend x : multiRequestSends) {
            responses.add(x.getResponse());
        }
        System.out.println("responses number = " + responses.size());
        return responses;

    }
    static String getValueReadWriteQuorum(int neededPortNumber, int currentPortNumber, String request, int quorumReadWrite) throws InterruptedException, IOException {
        List<String> responses = sendRequestToAllReplicas(neededPortNumber, currentPortNumber, request);
        Map<String, Integer> responsesCount = new HashMap<>();

        for (String response : responses) {
            if (responsesCount.containsKey(response) ){
                responsesCount.put(response, responsesCount.get(response) + 1);
            } else {
                responsesCount.put(response, 1);
            }
        }
        System.out.println("responsesCount = " + responsesCount);
        for (String key : responsesCount.keySet()) {

            if (responsesCount.get(key) >= quorumReadWrite){

                return key;
            }
        }
        return "Error 404 Not Found";
    }
    static LSMTree crash( int replicaID) throws IOException {
        File file = new File("/home/elemary/Projects/DataBaseManagement/Node_Number"+ currentPortNumber +"/ReplicaOf"+replicaID);
        if (file.exists()) {
            System.out.println("File exists and need to be reloaded in LSM tree ");
            LSMTree crashRecovery = new LSMTree(currentPortNumber, replicaID,maxMemTableSize, maxSegmentSize, true);
            File segments = new File("/home/elemary/Projects/DataBaseManagement/Node_Number" + currentPortNumber + "/ReplicaOf" + replicaID + "/Data/");
            if (segments.exists()) {
                File[] files = segments.listFiles();
                for (File file1 : files) {
                    if (file1.isFile()) {
                        crashRecovery.segmentIDs.add(Integer.valueOf(file1.getName().split("\\.")[0]));
                    }
                }
                System.out.println("segmentIDs = " + crashRecovery.segmentIDs);
            }

            File commitLog = new File("/home/elemary/Projects/DataBaseManagement/Node_Number" + currentPortNumber + "/ReplicaOf" + replicaID + "/"
                    + "commitLog" + replicaID + ".txt");
            if (commitLog.exists()) {
                Scanner myReader = null;
                try {
                    myReader = new Scanner(commitLog);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
                while (myReader.hasNextLine()) {
                    String data = myReader.nextLine();
                    List<String> dataSplit = Arrays.asList(data.split(","));
                    crashRecovery.memTable.insert(dataSplit.get(0), dataSplit.get(1));
                    System.out.println("Key = " + dataSplit.get(0) + " Value = " + dataSplit.get(1));
                }
            }
            System.out.println("Loading Bloom Filter");
          /*  InputStream inStream = new FileInputStream("/home/elemary/Projects/DataBaseManagement/Node_Number" + currentPortNumber + "/ReplicaOf" + replicaID + "/"
                    + "bloomFilter" + replicaID + ".txt");

            BloomFilter<String> bloomFilter
                    = BloomFilter.readFrom(inStream, Funnels.stringFunnel(Charset.defaultCharset()));
*/
            return crashRecovery;
        }
        return new LSMTree(currentPortNumber, replicaID,maxMemTableSize, maxSegmentSize, true);

    }
    static void set(String request, Socket sender,boolean withQuorum, int writeQuorum) throws IOException, InterruptedException {
        String data = request.substring(4, request.length() - 1);
        String key = data.split(",")[0];
        String value = data.split(",")[1];
        int neededPortNumber = keyPortNumber(key);
        String response = "";
        if (hasReplica(neededPortNumber, currentPortNumber)) {
            System.out.println("I am not in the correct node");
            response = getValueReadWriteQuorum(neededPortNumber, currentPortNumber, request, writeQuorum);
            sendStringToSocket(sender, response);
        } else { // Current Node has a replica.
            for (LSMTree lsmTree : lsmTrees) { // Find the right partition.
                if (lsmTree.getReplicaId() == neededPortNumber) {
                    lsmTree.commitLogs(key,value);

                    lsmTree.setValueOf(key, value);

                    if (withQuorum) {
                        response = getValueReadWriteQuorum(neededPortNumber, currentPortNumber, request, writeQuorum);
                        sendStringToSocket(sender, response);
                    }
                    else {
                        sendStringToSocket(sender, "Set Successful");
                    }
                    break;
                }
            }
        }
    }
    static String findValueFromPartitions(int neededPortNumber, String key) throws IOException {
        String value = "Error 404 Not Found";
        for (LSMTree lsmTree : lsmTrees) {
            if (lsmTree.getReplicaId() == neededPortNumber) {
                value = lsmTree.getValueOf(key);
                if (value != null) {
                    return value;
                }
            }
        }
       return value;
    }
    static int keyPortNumber(String key) {
        long hashCode = MurmurHash3.hash32x86(key.getBytes());
        System.out.println("Hash Code is : " + hashCode);
        Long nodeIndexOnRing = ringStructure.find_Node(hashCode);
        System.out.println("Get index in correspond Node : " + nodeIndexOnRing);
        int neededPortNumber = ringStructure.nodes_Ports.get(nodeIndexOnRing);
        System.out.println("neededPortNumber is " + neededPortNumber + "  #######  " + "This Port is  " + currentPortNumber);
        return neededPortNumber;
    }
    static void get(String request, Socket sender, boolean withQuorum, int readQuorum) throws IOException, InterruptedException {
        String data = request.substring(4, request.length() - 1);
        String key = data.split(",")[0];
        int neededPortNumber = keyPortNumber(key);
        String response = "";
        if (!hasReplica(neededPortNumber)) {
            System.out.println("I am not in the correct node");
            response = getValueReadWriteQuorum(neededPortNumber, request, readQuorum);
            Requests.sendStringToSocket(sender, response);
        } else {
            System.out.println("I am in the correct node");

            for (LSMTree lsmTree : lsmTrees) {
                if (lsmTree.getReplicaId() == neededPortNumber) {
                    // ask the server to get the value from other nodes
                    if (withQuorum) {
                        currentValue = getValueReadWriteQuorum(neededPortNumber, currentPortNumber, request, readQuorum);
                    } else {
                        currentValue = lsmTree.getValueOf(key);
                        if (currentValue == null) {
                            currentValue = "Error 404 Not Found";
                        }
                    }
                    sendStringToSocket(sender, currentValue);
                    System.out.println("The key " + key + " and value " + currentValue + " is set in the LSMTree " + lsmTree.getReplicaId());
                    break;
                }
            }
        }
    }
    static void doCompaction() throws IOException, InterruptedException {
        new java.util.Timer().schedule(
                new java.util.TimerTask() {
                    @Override
                    public void run() {
                        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!! Compaction Started");
                        for (LSMTree lsmTree : lsmTrees) {
                            try {
                                sem.acquire();
                                lsmTree.startCompaction();
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!! Compaction Finished");
                    }

                },500, (long) nodeNumber * numberOfNodes * 500 + (long) numberOfNodes * nodeNumber*500
        );
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Server configurations 6 6 20 3 10 5 2 2 1;
        nodeNumber = Integer.parseInt(args[0]);
        numberOfNodes = Integer.parseInt(args[1]);
        numberOfVirtualNodes = Integer.parseInt(args[2]);
        replicationFactor = Integer.parseInt(args[3]);
        maxSegmentSize = Integer.parseInt(args[4]);
        maxMemTableSize = Integer.parseInt(args[5]);
        int writeQuorum = Integer.parseInt(args[6]);
        int readQuorum = Integer.parseInt(args[7]);
        int approach = Integer.parseInt(args[8]);
        int isAddedNode = Integer.parseInt(args[9]);
        final int START_PORT = 5000;
        currentPortNumber = START_PORT + nodeNumber;
        boolean withCrashRecovery;

        // Print server's configurations.
        System.out.println("Port Number = " + currentPortNumber);
        System.out.println("Node Number = " + nodeNumber);
        System.out.println("Number Of nodes : " + numberOfNodes);
        System.out.println("Number Of Virtual Nodes : " + numberOfVirtualNodes);
        System.out.println("Replication Factor : " + replicationFactor);
        System.out.println("Max Segment Size : " + maxSegmentSize);

        if (approach != 0){
            System.out.println("With Crash Recovery");
            withCrashRecovery = true;
        }
        else {
            System.out.println("Without Crash Recovery");
            withCrashRecovery = false;
        }

        // Create ring structure.
        ringStructure = RingStructure.getInstance(numberOfNodes, numberOfVirtualNodes, replicationFactor);
        ringStructure.buildMap(numberOfVirtualNodes);
        ringStructure.nodesReplicasMapping.printWhichReplicasBelongToNode();
        System.out.println("Ring Structure is built successfully");
        lsmTrees= new ArrayList<>();
        Map<Integer, List<Integer>> nodeReplicas = nodesReplicasMapping.whichReplicasBelongToNode;
        if (isAddedNode==1){
            System.out.println("Getting other replicas and sending the partition to other nodes  "+ nodeNumber);
            Rearrange.start(nodeNumber, replicationFactor, "/home/elemary/Projects/DataBaseManagement/");
            int t=5;
            while (t-->0){
                System.out.print("_ ");
                Thread.sleep(500);
            }
        }
        for (int i = 0; i < replicationFactor; i++) {
            System.out.println("Replica " + i + " is in port " + nodeReplicas.get(currentPortNumber).get(i));
            if (withCrashRecovery){
                LSMTree temp = crash(nodeReplicas.get(currentPortNumber).get(i));
                lsmTrees.add(temp);
            } else {
                lsmTrees.add(new LSMTree(currentPortNumber, nodeReplicas.get(currentPortNumber).get(i), maxMemTableSize, maxSegmentSize, withCrashRecovery));
            }
        }
        System.out.println("--- Set Up the server ");
        int t=5;
        while (t-->0){
            System.out.print("_ ");
            Thread.sleep(1000);
        }
        doCompaction();
        if (currentPortNumber==5001){
            Requests.sendToPort(7777,args[1]+" "+args[2]+" "+args[3], false);
        }
        try (ServerSocket serverSocket = new ServerSocket(currentPortNumber)) {

            while (true) {
                System.out.println(" ***** I am listening .... ");

                boolean withQuorum = true;
                for (LSMTree lsmTree : lsmTrees) {
                    System.out.println("LSM Tree " + lsmTree.getReplicaId()+" memTable is : ");
                    lsmTree.memTable.print();
                }
                // Getting request from client
                Socket sender = serverSocket.accept();
                String request = Requests.getInputFromSocket(sender);
                System.out.println("Received request from  : " + sender.getPort() + " :  " + request);
                // Sender is a server not a client.
                if (request.charAt(0)=='*') {
                    withQuorum = false;
                    request = request.substring(1);
                }
                if (request.startsWith("set")) {
                        sem.acquire();
                        set(request, sender, withQuorum, readQuorum);
                        sem.release();
                } else if (request.startsWith("get")) {
                    if (sem.tryAcquire()){
                        get(request, sender, withQuorum, writeQuorum);
                        sem.release();
                    }
                    else {
                        Requests.sendStringToSocket(sender, "Error 503 Service Unavailable");
                    }
                }else if (request.startsWith("addNode")){
                    sem.acquire();
                    AddNodeThread addNodeThread= new AddNodeThread(lsmTrees, request, currentPortNumber);
                    addNodeThread.start();
                }
            }
        }
    }
}

