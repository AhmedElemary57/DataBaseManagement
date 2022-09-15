package org.example;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.codec.digest.MurmurHash3;

import java.net.*;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.Semaphore;

public class Server {
    static String Path = System.getProperty("user.dir");
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

        if (replicasPosition.contains(currentPortNumber)){
            String key = "";
            String myResponse = "Set Successful";
            if (request.startsWith("get")){
                key = request.substring(4, request.length() - 1);
                myResponse = findValueFromPartitions(partitionID ,key );
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
    static String getValueReadWriteQuorum(int neededPortNumber, String request, int quorumReadWrite) throws InterruptedException, IOException {
        List<String> responses = sendRequestToAllReplicas(neededPortNumber, request);
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
    static LSMTree loadSegments(int replicaID, LSMTree crashRecovery) {
        File segments = new File("./Node_Number" + currentPortNumber + "/ReplicaOf" + replicaID + "/Data/");
        if (segments.exists()) {
            File[] files = segments.listFiles();
            for (File file1 : files) {
                if (file1.isFile()) {
                    crashRecovery.segmentIDs.add(Integer.valueOf(file1.getName().split("\\.")[0]));
                }
            }
            System.out.println("segmentIDs = " + crashRecovery.segmentIDs);
        }
        return crashRecovery;
    }
    static LSMTree loadCommitLog(int replicaID, LSMTree crashRecovery) {
        File commitLog = new File("./Node_Number" + currentPortNumber + "/ReplicaOf" + replicaID + "/"
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
        return crashRecovery;
    }
    public static  LSMTree loadBloomFilter(int replicaID, LSMTree crashRecovery) throws IOException {

        System.out.println("Loading Bloom Filter");
        InputStream inStream = new FileInputStream("./Node_Number" + currentPortNumber + "/ReplicaOf" + replicaID + "/"
                + "bloomFilter" + replicaID + ".txt");

        BloomFilter<String> bloomFilter
                = BloomFilter.readFrom(inStream, Funnels.stringFunnel(Charset.defaultCharset()));
        crashRecovery.bloomFilter = bloomFilter;
        return crashRecovery;
    }
    static LSMTree crash(int replicaID) throws IOException {
        File file = new File("./Node_Number"+ currentPortNumber +"/ReplicaOf"+replicaID);
        if (file.exists()) {
            System.out.println("File exists and need to be reloaded in LSM tree ");
            LSMTree crashRecovery = new LSMTree(currentPortNumber, replicaID,maxMemTableSize, maxSegmentSize, true);
            //get files from Data folder
            File segments = new File("./Node_Number"+ currentPortNumber +"/ReplicaOf"+replicaID+"/Data/");
            if (segments.exists()) {
                File[] files = segments.listFiles();
                for (File file1 : files) {
                    if (file1.isFile()) {
                        crashRecovery.segmentIDs.add(Integer.valueOf(file1.getName().split("\\.")[0]));
                        System.out.println("File name is " + file1.getName());
                    }
                }
            }
            File commitLog = new File("./Node_Number"+ currentPortNumber +"/ReplicaOf"+replicaID+"/"+"commitLog"+replicaID+".txt");
            if (commitLog.exists()) {
                Scanner myReader = new Scanner(commitLog);
                while (myReader.hasNextLine()) {
                    String data = myReader.nextLine();
                    List<String> dataSplit = Arrays.asList(data.split(","));
                    crashRecovery.memTable.insert(dataSplit.get(0), dataSplit.get(1));
                    System.out.println("Key = " + dataSplit.get(0) + " Value = " + dataSplit.get(1));
                }
            }
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
        if (!hasReplica(neededPortNumber)) {
            System.out.println("I am not in the correct node");
            response = getValueReadWriteQuorum(neededPortNumber, request, writeQuorum);
            Requests.sendStringToSocket(sender, response);
        } else {
            for (LSMTree lsmTree : lsmTrees) {
                if (lsmTree.getReplicaId() == neededPortNumber) {
                    lsmTree.commitLogs(key,value);
                    lsmTree.setValueOf(key, value);
                    if (withQuorum) {
                        response = getValueReadWriteQuorum(neededPortNumber, request, writeQuorum);
                        Requests.sendStringToSocket(sender, response);
                    }
                    else {
                        Requests.sendStringToSocket(sender, "Set Successful");
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
        int hashCode = MurmurHash3.hash32x86(key.getBytes());
        System.out.println("Hash Code is : " + hashCode);
        Integer nodeIndexOnRing = ringStructure.find_Node(hashCode);
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
                        currentValue = getValueReadWriteQuorum(neededPortNumber, request, readQuorum);
                    } else {
                        currentValue = lsmTree.getValueOf(key);
                        if (currentValue == null) {
                            currentValue = "Error 404 Not Found";
                        }
                    }
                    Requests.sendStringToSocket(sender, currentValue);
                    System.out.println("The key " + key + " and value " + currentValue + " is set in the LSMTree " + lsmTree.getReplicaId());
                    break;
                }
            }
        }
    }
    static void doCompaction() throws IOException, InterruptedException {
        sem.acquire();
        new java.util.Timer().schedule(
                new java.util.TimerTask() {
                    @Override
                    public void run() {
                        for (LSMTree lsmTree : lsmTrees) {
                            try {
                                lsmTree.startCompaction();
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                },1000, (long) nodeNumber * numberOfNodes * 2000 + (long) numberOfNodes * nodeNumber * 2000
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
        ringStructure = RingStructure.getInstance(numberOfNodes, numberOfVirtualNodes, replicationFactor);
        ringStructure.buildMap(numberOfVirtualNodes);
        nodesReplicasMapping.printWhichReplicasBelongToNode();
        System.out.println("Ring Structure is built successfully");
        lsmTrees= new ArrayList<>();
        Map<Integer, List<Integer>> nodeReplicas = nodesReplicasMapping.whichReplicasBelongToNode;

        for (int i = 0; i < replicationFactor; i++) {
            if (withCrashRecovery){
                LSMTree temp = crash(nodeReplicas.get(currentPortNumber).get(i));
                lsmTrees.add(temp);
            } else {
                lsmTrees.add(new LSMTree(currentPortNumber, nodeReplicas.get(currentPortNumber).get(i), maxMemTableSize, maxSegmentSize, withCrashRecovery));
            }
        }
        System.out.println("Set Up the server ");
        int t=10;
        while (t-->0){
            System.out.print("_ ");
            Thread.sleep(100);
        }
        System.out.println("\n");
        System.out.println("Server start its automated compaction each " + numberOfNodes * nodeNumber * 2 + " s");
        doCompaction();
        if (currentPortNumber==5001){
            Requests.sendToPort(7777,args[1]+" "+args[2]+" "+args[3], false);
        }

        try (ServerSocket serverSocket = new ServerSocket(currentPortNumber)) {

            while (true) {

                System.out.println("-------------------------  Waiting for client request");

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

