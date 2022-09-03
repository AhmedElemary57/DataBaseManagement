package org.example;

import org.apache.commons.codec.digest.MurmurHash3;

import java.net.*;
import java.io.*;
import java.util.*;


public class Server {
    static RingStructure ringStructure;
    static String currentValue = "Error 404 Not Found";


    /**
     * @param portNumber : The destination port
     * @param waitAnswer : Flag for checking to wait for response.
     * @return response if flag waitAnswer, null otherwise.
     */
    public static String sendToPort(int portNumber, String request, boolean waitAnswer) throws IOException {
        // Socket initialized
        System.out.println("Make socket to port number " + portNumber);
        try ( Socket serverSocket = new Socket("localhost", portNumber)){
            sendStringToSocket(serverSocket, request);
            System.out.println("Message sent to port number " + portNumber);

            if (waitAnswer) {
                String response = getInputFromSocket(serverSocket);
                System.out.println(" ----- Received from Server : " + response);
                return response;
            }
            return null;
        }catch (IOException e) {
            System.out.println("Error in sending to port number " + portNumber);
            return "Error 404 Not Found";
        }
    }

    static String getInputFromSocket(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        return dataInputStream.readUTF();
    }

    static void sendStringToSocket(Socket socket, String message) throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        dataOutputStream.writeUTF(message);
        dataOutputStream.flush();
    }
    /**
     * @param list: List to choose from it unique random values.
     * @param k:    K unique numbers.
     * @return returns List of unique numbers chosen form the list.
     */
    static List<Integer> randomFromList(List<Integer> list, int k) {
        List<Integer> result = new ArrayList<>();
        Random rn = new Random();

        System.out.println(" List size == " + list.size());
        System.out.println(" K == " + k);

        for (int i = 0; i < k; i++) {
            int randomPosition = rn.nextInt(list.size());
            result.add(list.get(randomPosition));
            list.remove(randomPosition);
        }
        return result;
    }


    static boolean hasReplica(int partitionID, int currentPortNumber) {
        List<Integer> replicas = ringStructure.nodesReplicasMapping.whichReplicasBelongToNode.get(currentPortNumber);
        for (int replica : replicas) {
            if (replica == partitionID) {
                return true;
            }
        }
        return false;
    }

    private static int getPositionOfTheReplica(int partitionID) {
        List<Integer> replicas = ringStructure.nodesReplicasMapping.nodeReplicasPositionMapping.get(partitionID);
        Random rand = new Random();
        int randomReplica = rand.nextInt(replicas.size());
        return replicas.get(randomReplica);
    }


    static List<String> sendRequestToAllReplicas(int neededReplicaId, int currentPortNumber, String request) throws IOException, InterruptedException {
        List<Integer> replicasPosition = ringStructure.nodesReplicasMapping.getPositionReplicasOfNode(neededReplicaId);
        System.out.println("replicasPosition = " + replicasPosition.size());
        List<String> responses = new ArrayList<>();


        if (replicasPosition.contains(Integer.valueOf(currentPortNumber))){
            replicasPosition.remove(Integer.valueOf(currentPortNumber));
            responses.add("Set Successful");
        }
        //TODO do not know if we should do concurrency here ?? or not
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
        for (String key : responsesCount.keySet()) {

            if (responsesCount.get(key) >= quorumReadWrite){

                return key;
            }
        }
        return "Error 404 Not Found";
    }
    static LSMTree crash(int currentPortNumber,int replicaID, int maxMemTableSize, int maxSegmentSize) throws IOException {
        File file = new File("/home/elemary/Projects/DataBaseManagement/Node_Number"+ currentPortNumber +"/ReplicaOf"+replicaID+"/"+"commitLog"+replicaID+".txt");
        if (file.exists()) {
            System.out.println("File exists and need to be reloaded in LSM tree ");
            LSMTree crashRecovery = new LSMTree(currentPortNumber, replicaID,maxMemTableSize, maxSegmentSize, true);
            Scanner myReader = new Scanner(file);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                List<String> dataSplit = Arrays.asList(data.split(","));
                crashRecovery.memTable.insert(dataSplit.get(0), dataSplit.get(1));
                System.out.println("Key = " + dataSplit.get(0) + " Value = " + dataSplit.get(1));
            }
            return crashRecovery;

        }
        return new LSMTree(currentPortNumber, replicaID,maxMemTableSize, maxSegmentSize, true);

    }
    static void set(String request, int currentPortNumber, Socket sender, List<LSMTree> lsmTrees, boolean withQuorum, int writeQuorum) throws IOException, InterruptedException {
        // Praising Request.
        String data = request.substring(4, request.length() - 1);
        String key = data.split(",")[0];
        String value = data.split(",")[1];
        System.out.println("key = " + key);
        System.out.println("value = " + value);

        // Hash the key.
        long hashCode = MurmurHash3.hash32x86(key.getBytes());
        System.out.println("Hash Code is : " + hashCode);

        // Get the virtual node and its main partitionID (port number).
        Long vnIndex = ringStructure.find_Node(hashCode);
        System.out.println("Virtual Node Index = " + vnIndex);
        int neededPortNumber = ringStructure.nodes_Ports.get(vnIndex);
        System.out.println("Correct Node Port : ------->" + neededPortNumber);
        System.out.println("neededPortNumber is " + neededPortNumber + "  #######  " + "This Port is  " + currentPortNumber);

        // Check if current node has a replica.
        String response = "";
        if (!hasReplica(neededPortNumber, currentPortNumber)) {
            System.out.println("I am not in the correct node");
            response = getValueReadWriteQuorum(neededPortNumber, currentPortNumber, request, writeQuorum);
            sendStringToSocket(sender, response);
        } else { // Current Node has a replica.
            for (LSMTree lsmTree : lsmTrees) { // Find the right partition.
                if (lsmTree.getReplicaId() == neededPortNumber) {
                    // commit and end request.
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

    static void get(String request, int currentPortNumber, Socket sender, List<LSMTree> lsmTrees, boolean withQuorum, int readQuorum) throws IOException, InterruptedException {
        String data = request.substring(4, request.length() - 1);
        String key = data.split(",")[0];
        long hashCode = MurmurHash3.hash32x86(key.getBytes());
        System.out.println("Hash Code is : " + hashCode);
        Long nodeIndexOnRing = ringStructure.find_Node(hashCode);
        System.out.println("Get index in correspond Node : " + nodeIndexOnRing);
        int neededPortNumber = ringStructure.nodes_Ports.get(nodeIndexOnRing);
        System.out.println("Correct Node Port : ------->" + neededPortNumber);
        System.out.println("neededPortNumber is " + neededPortNumber + "  #######  " + "This Port is  " + currentPortNumber);
        String response = "";
        if (!hasReplica(neededPortNumber, currentPortNumber)) {
            System.out.println("I am not in the correct node");
            response = getValueReadWriteQuorum(neededPortNumber, currentPortNumber, request, readQuorum);
            sendStringToSocket(sender, response);
        } else {
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

    public static void main(String[] args) throws IOException, InterruptedException {
        // Server configurations
        int nodeNumber = Integer.parseInt(args[0]);
        int numberOfNodes = Integer.parseInt(args[1]);
        int numberOfVirtualNodes = Integer.parseInt(args[2]);
        int replicationFactor = Integer.parseInt(args[3]);
        int maxSegmentSize = Integer.parseInt(args[4]);
        int maxMemTableSize = Integer.parseInt(args[5]);
        int writeQuorum = Integer.parseInt(args[6]);
        int readQuorum = Integer.parseInt(args[7]);
        int approach = Integer.parseInt(args[8]);
        final int START_PORT = 5000;
        int currentPortNumber = START_PORT + nodeNumber;
        boolean withCrashRecovery;

        // Print server's configurations.
        System.out.println("Port Number = " + currentPortNumber);
        System.out.println("Node Number = " + nodeNumber);
        System.out.println("Number Of nodes : " + numberOfNodes);
        System.out.println("Number Of Virtual Nodes : " + numberOfVirtualNodes);
        System.out.println("Replication Factor : " + replicationFactor);
        System.out.println("Max Segment Size : " + maxSegmentSize);
        if (approach!=0){
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
        System.out.println("Ring Structure is built");

        // Get LSM for every partition
        List<LSMTree> lsmTrees = new ArrayList<>();
        Map<Integer, List<Integer>> nodeReplicas = ringStructure.nodesReplicasMapping.whichReplicasBelongToNode;
        for (int i = 0; i < replicationFactor; i++) {
            System.out.println("Replica " + i + " is in port " + nodeReplicas.get(currentPortNumber).get(i));
            if (withCrashRecovery){
                LSMTree temp = crash(currentPortNumber, nodeReplicas.get(currentPortNumber).get(i), maxSegmentSize, maxMemTableSize);
                lsmTrees.add(temp);
            } else {
                lsmTrees.add(new LSMTree(currentPortNumber, nodeReplicas.get(currentPortNumber).get(i), maxMemTableSize, maxSegmentSize, withCrashRecovery));
            }
        }

        try (ServerSocket serverSocket = new ServerSocket(currentPortNumber)) {

            while (true) {
                System.out.println("I am listening .... ");

                boolean withQuorum = true;
                for (LSMTree lsmTree : lsmTrees) {
                    System.out.println("LSM Tree " + lsmTree.getReplicaId()+" memTable is : ");
                    lsmTree.memTable.print();
                }
                // Getting request from client
                Socket sender = serverSocket.accept();
                String request = getInputFromSocket(sender);

                System.out.println("Received request from  : " + sender.getPort() + " :  " + request);
                // Sender is a server not a client.
                if (request.charAt(0)=='*') {
                    withQuorum = false;
                    request = request.substring(1);
                }
                if (request.startsWith("set")) {
                    set(request, currentPortNumber, sender, lsmTrees, withQuorum, readQuorum);
                } else if (request.startsWith("get")) {
                    get(request, currentPortNumber, sender, lsmTrees, withQuorum, writeQuorum);
                }else if (request.startsWith("addNode")){
                    ringStructure.addNode();
                    ringStructure.nodesReplicasMapping.printWhichReplicasBelongToNode();
                    ringStructure.nodesReplicasMapping.printChangedNodes();
                    String newNodePort = request.split(" ")[1];
                    String newPartitionPath = "/home/elemary/Projects/DataBaseManagement/Node_Number"+ newNodePort +"/ReplicaOf"+ newNodePort +"/Data/";
                    String oldPartitionPath = "/home/elemary/Projects/DataBaseManagement/Node_Number"+ currentPortNumber  +"/ReplicaOf"+ currentPortNumber +"/Data/";
                    Rehash.createNewPartition(newPartitionPath, oldPartitionPath);
                }
                System.out.println(request);
            }
        }
    }
}

