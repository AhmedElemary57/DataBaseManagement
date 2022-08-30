package org.example;

import org.apache.commons.codec.digest.MurmurHash3;

import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;;import static java.lang.Thread.sleep;

public class Server {
    static RingStructure ringStructure;
    static String currentValue;

    public static String startSendingRequestToOtherServer(int portNumber, String request,boolean waitAnswer) throws IOException {
        //I think there is a problem here a should destroy the socket after using if

        System.out.println("make socket to port number "+ portNumber);
        Socket serverSocket = new Socket("localhost", portNumber);

        // How to pass a parameter to a new thread ???
        sendStringToSocket(serverSocket,request);
        System.out.println("Message sent to the client");
        if(waitAnswer) {
            String response = getInputFromSocket(serverSocket);
            System.out.println(" ----- Received from Server : " + response);
            return response;
        }
        return null;
    }
    static String getInputFromSocket(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        // create a DataInputStream so we can read data from it.
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        // read the message from the socket
        String message = dataInputStream.readUTF();// send the message
        System.out.println(" ----- Received from Server : " +message);
        return message;
    }
    static void sendStringToSocket(Socket socket, String message) throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        // create a data output stream from the output stream so we can send data through it
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        // write the message we want to send
        dataOutputStream.writeUTF(message);
        dataOutputStream.flush(); // send the message
    }
    /**
     * Search the location of the needed port in the other nodes so we can know if it is available or not in current node
     *
     * */
    private static boolean isKeyInOneOfTheReplicas(int neededPortNumber, int portNumber) {
        List<Integer> replicas = ringStructure.nodesReplicasMapping.whichReplicasBelongToNode.get(portNumber);
        for (int replica : replicas) {
            if (replica == neededPortNumber) {
                return true;
            }
        }
        return false;
    }
    static void writeToOtherReplicas(int currentPortNumber, int replicaId, String key, String value, int writeQuorum) {
        System.out.println("writeToOtherReplicas");
        List<Integer> replicasPosition = ringStructure.nodesReplicasMapping.getPositionReplicasOfNode(replicaId);
        replicasPosition.remove(new Integer(currentPortNumber));
        //chose a tow different replicas from replicasPosition
        List<Integer> replicasPositionToWrite = new ArrayList<>();
        for (int i = 0; i < writeQuorum; i++) {
            int randomPosition = (int) (Math.random() * replicasPosition.size());
            if ( (!replicasPositionToWrite.contains(replicasPosition.get(randomPosition)) && !replicasPosition.get(randomPosition).equals(currentPortNumber))) {
                replicasPositionToWrite.add(replicasPosition.get(randomPosition));
                replicasPosition.remove(new Integer(randomPosition));
            }else {
                System.out.println("randomPosition is Not valid");
                i--;
            }
        }
        for (int replicaPosition : replicasPositionToWrite) {
            WriteQuorumThread writeQuorumThread = new WriteQuorumThread(replicaPosition, key, value);
            writeQuorumThread.start();

        }

    }
    public static void main(String[] args) throws IOException, InterruptedException {
        // ServerSocket serverSocket2 = new ServerSocket(5888);

       // ServerSocket serverSocket = new ServerSocket(4747);
        int numberOfNodes=Integer.valueOf(args[1]);
        System.out.println("Number Of nodes : "+numberOfNodes);
        int nodeNumber=Integer.valueOf(args[0]);
        int numberOfVirtualNodes=Integer.valueOf(args[2]), replicationFactor=Integer.valueOf(args[3]) ,maxSegmentSize=Integer.valueOf(args[4]);
        ringStructure= RingStructure.getInstance(numberOfNodes,numberOfVirtualNodes, replicationFactor);
        ringStructure.buildMap(10);
        ringStructure.nodesReplicasMapping.printWhichReplicasBelongToNode();
        System.out.println(".^^^^^ Node number : "+args[0]);

        //Here we will start listening to any one who wants to connect to the server
        int portNumber=5000+nodeNumber;
        List<LSMTree> lsmTrees = new ArrayList<LSMTree>();
        Map<Integer,List<Integer>> nodeReplicas= ringStructure.nodesReplicasMapping.whichReplicasBelongToNode;
        for (int i=0;i<replicationFactor;i++){
            lsmTrees.add( new LSMTree(portNumber,nodeReplicas.get(portNumber).get(i),2,maxSegmentSize) );
        }

        ServerSocket serverSocket = new ServerSocket(portNumber);

        while (true){
            System.out.println("I am listening .... ");
            boolean serverRequest=false;
            Socket clientSocket = serverSocket.accept();
            String request = getInputFromSocket(clientSocket);
            System.out.println("Received request from client : " + request);
            if(request.charAt(0)=='*'){
                request=request.substring(1);
                serverRequest=true;
            }
            if(request.substring(0,3).equals("set")){
                String data =request.substring(4,request.length()-1);
                String key = data.split(",")[0];
                String value = data.split(",")[1];

                int hashCode= MurmurHash3.hash32x86(key.getBytes());
                System.out.println("Hash Code is : "+hashCode);

                int nodeIndexOnRing=ringStructure.find_Node(hashCode);
                System.out.println("Set index in correspond Node : "+ nodeIndexOnRing);
                int neededPortNumber= ringStructure.nodes_Ports.get(nodeIndexOnRing);

                System.out.println("Correct Node Port : ------->" + neededPortNumber);
                System.out.println("neededPortNumber is " + neededPortNumber +"  #######  " +"This Port is  "+portNumber);

                if (!isKeyInOneOfTheReplicas(neededPortNumber,portNumber) ){
                    //setRequestToOtherServer(neededPortNumber,request);
                    startSendingRequestToOtherServer(neededPortNumber,request,true);
                    sendStringToSocket(clientSocket,"Set Successful");
                }else {
                    for (LSMTree lsmTree : lsmTrees) {
                        if (lsmTree.getReplicaId()==neededPortNumber){
                            lsmTree.commitLogs(key,value);
                            sendStringToSocket(clientSocket,"Set Successful");
                            Thread.sleep(1000);
                            lsmTree.put(key,value);
                            System.out.println("The key "+key+" and value "+value+" is set in the LSMTree "+lsmTree.getReplicaId());
                            // case first write to avoid infinite loop so we can use flag to know if we have written to the other replicas or not
                            if (!serverRequest){
                                writeToOtherReplicas(portNumber,neededPortNumber,key,value,2);
                            }
                            sendStringToSocket(clientSocket,"Set Successful To all quorum requested");
                            break;
                        }
                    }
                }
            }else if(request.substring(0,3).equals("get")){

                String data =request.substring(4,request.length()-1);
                String key = data.split(",")[0];
                int hashCode= MurmurHash3.hash32x86(key.getBytes());
                System.out.println("Hash Code is : "+hashCode);
                int nodeIndexOnRing=ringStructure.find_Node(hashCode);
                System.out.println("Get index in correspond Node : "+ nodeIndexOnRing);
                int neededPortNumber= ringStructure.nodes_Ports.get(nodeIndexOnRing);
                System.out.println("Correct Node Port : ------->" + neededPortNumber);
                System.out.println("neededPortNumber is " + neededPortNumber +"  #######  " +"This Port is  "+portNumber);
                if (neededPortNumber != portNumber){

                    //getRequestToOtherServer(neededPortNumber,request);
                    currentValue = startSendingRequestToOtherServer(neededPortNumber,request,true);
                    sendStringToSocket(clientSocket,currentValue);

                }else {
          //          currentValue = lsmTree.getValueOf(key);
                    System.out.println("Get Successful");
                    sendStringToSocket(clientSocket,currentValue);
                }

            }
        }

    }




}
