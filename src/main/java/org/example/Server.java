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
        Socket serverSocket = new Socket("localhost", portNumber);

        // Send request
        sendStringToSocket(serverSocket, request);
        System.out.println("Message sent to port number " + portNumber);

        if (waitAnswer) {
            String response = getInputFromSocket(serverSocket);
            System.out.println(" ----- Received from Server : " + response);
            return response;
        }
        return null;
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
        for (int i = 0; i < k; i++) {
            Random rn = new Random();
            int randomPosition = rn.nextInt(list.size());
            result.add(list.get(randomPosition));
            list.remove(Integer.valueOf(randomPosition));
        }
        return result;
    }

    static String getValueReadQuorum(int neededPortNumber, int currentPortNumber, String key, int quorumRead) throws InterruptedException {
        System.out.println("Reading From Other Replicas...");
        List<Integer> dataPorts = ringStructure.nodesReplicasMapping.getPositionReplicasOfNode(neededPortNumber);
        dataPorts.remove(Integer.valueOf(currentPortNumber));

        List<Integer> chosenReplicas = randomFromList(dataPorts, quorumRead);

        List<ReadQuorumThread> readQuorum = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < chosenReplicas.size(); i++) {
            readQuorum.add(new ReadQuorumThread(chosenReplicas.get(i), key));
            threads.add(new Thread(readQuorum.get(i)));
            threads.get(i).start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        List<String> values = new ArrayList<>();
        List<String> versions = new ArrayList<>();
        // All threads ended
        for (ReadQuorumThread x : readQuorum) {
            values.add(x.getValue());
            versions.add(x.getVersion());
        }
        String max = Collections.max(versions);
        System.out.println("Max version : " + max + " for key " + key+ " Value : " + values.get(versions.indexOf(max)));
        return values.get(versions.indexOf(max));
    }

    /**
     * checks if current node has the desired partition.
     */
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

    static void writeToOtherReplicas(int currentPortNumber, int replicaId, String key, String value, int writeQuorum) {
        System.out.println("writeToOtherReplicas");
        List<Integer> replicasPosition = ringStructure.nodesReplicasMapping.getPositionReplicasOfNode(replicaId);
        replicasPosition.remove(Integer.valueOf(currentPortNumber));
        List<Integer> replicasPositionToWrite = randomFromList(replicasPosition, writeQuorum);
        for (int replicaPosition : replicasPositionToWrite) {
            WriteQuorumThread writeQuorumThread = new WriteQuorumThread(replicaPosition, key, value);
            writeQuorumThread.start();
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
        final int START_PORT = 5000;
        int portNumber = START_PORT + nodeNumber;

        // Print server's configurations.
        System.out.println("Node Number = " + nodeNumber);
        System.out.println("Number Of nodes : " + numberOfNodes);
        System.out.println("Number Of Virtual Nodes : " + numberOfVirtualNodes);
        System.out.println("Replication Factor : " + replicationFactor);
        System.out.println("Max Segment Size : " + maxSegmentSize);

        // Create ring structure.
        ringStructure = RingStructure.getInstance(numberOfNodes, numberOfVirtualNodes, replicationFactor);
        ringStructure.buildMap(numberOfVirtualNodes);
        ringStructure.nodesReplicasMapping.printWhichReplicasBelongToNode();


        // Get LSM for every partition
        List<LSMTree> lsmTrees = new ArrayList<>();
        Map<Integer, List<Integer>> nodeReplicas = ringStructure.nodesReplicasMapping.whichReplicasBelongToNode;
        for (int i = 0; i < replicationFactor; i++) {
            lsmTrees.add(new LSMTree(portNumber, nodeReplicas.get(portNumber).get(i), maxMemTableSize, maxSegmentSize));
        }

        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {

            while (true) {
                System.out.println("I am listening .... ");

                boolean serverRequest = false;

                // Getting request from client
                Socket sender = serverSocket.accept();
                String request = getInputFromSocket(sender);
                System.out.println("Received request from  : " + sender.getPort() + " :  " + request);

                // Sender is a server not a client.
                if (request.charAt(0)=='*') {
                    serverRequest = true;
                }
                if (request.startsWith("set")) {
                    set(request, portNumber, sender, lsmTrees, serverRequest, writeQuorum);
                } else if (request.startsWith("get")) {
                    get(request, portNumber, sender, lsmTrees, serverRequest, readQuorum);
                }
            }
        }
    }
}
