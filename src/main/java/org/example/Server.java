package org.example;

import org.apache.commons.codec.digest.MurmurHash3;

import java.net.*;
import java.io.*;;
public class Server {

    static String currentValue;

    public static String startSendingRequestToOtherServer(int portNumber, String request) throws IOException {
        //I think there is a problem here a should destroy the socket after using if

        System.out.println("make socket to port number "+ portNumber);
        Socket serverSocket = new Socket("localhost", portNumber);

        // How to pass a parameter to a new thread ???
        sendStringToSocket(serverSocket,request);
        System.out.println("Message sent to the client");

        String response = getInputFromSocket(serverSocket);
        System.out.println(" ----- Received from Server : " +response);

        return response;
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


    public static void main(String[] args) throws IOException {
        // ServerSocket serverSocket2 = new ServerSocket(5888);

       // ServerSocket serverSocket = new ServerSocket(4747);
        int numberOfNodes=Integer.valueOf(args[1]);
        System.out.println("Number Of nodes : "+numberOfNodes);
        int nodeNumber=Integer.valueOf(args[0]);
        int numberOfVirtualNodes=Integer.valueOf(args[2]),replicationFactor=3;
        RingStructure ringStructure= RingStructure.getInstance(numberOfNodes,numberOfVirtualNodes, replicationFactor);
        ringStructure.buildMap(10);
        ringStructure.nodesReplicasMapping.printWhichReplicasBelongToNode();
        System.out.println(".^^^^^ Node number : "+args[0]);

        //Here we will start listening to any one who wants to connect to the server
        int portNumber=5000+nodeNumber;
        LSMTree lsmTree = new LSMTree(String.valueOf(portNumber), String.valueOf(portNumber) ,2);

        ServerSocket serverSocket = new ServerSocket(portNumber);
        while (true){
            System.out.println("I am listening .... ");
            Socket clientSocket = serverSocket.accept();
            String request = getInputFromSocket(clientSocket);
            System.out.println("Received request from client : " + request);

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
                if (neededPortNumber != portNumber){
                    //setRequestToOtherServer(neededPortNumber,request);
                    startSendingRequestToOtherServer(neededPortNumber,request);
                    sendStringToSocket(clientSocket,"Set Successful");
                }else {
                    System.out.println("Set Successful");
                    lsmTree.put(key,value);
                    System.out.println("Key : "+key);
                    System.out.println("Value : "+value);
                    System.out.println(lsmTree.memTable.toString());
                    sendStringToSocket(clientSocket,"Set Successful");
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
                    currentValue = startSendingRequestToOtherServer(neededPortNumber,request);
                    sendStringToSocket(clientSocket,currentValue);

                }else {
                    currentValue = lsmTree.getValueOf(key);
                    System.out.println("Get Successful");
                    sendStringToSocket(clientSocket,currentValue);
                }

            }
        }

    }


}
