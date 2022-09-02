package org.example;

import java.io.*;
import java.lang.reflect.Type;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.Scanner;
import java.net.InetSocketAddress;
public class Client {
    static final int START_PORT = 5000;

    int portNumber;
    int numberOfNodes;
    RingStructure ringStructure;
    public Client(int portNumber,int numberOfNodes) {
        this.portNumber = portNumber;
        this.numberOfNodes = numberOfNodes;
        int numberOfVirtualNodes = 20;
        int replicationFactor = 3;
        ringStructure = RingStructure.getInstance(numberOfNodes,numberOfVirtualNodes,replicationFactor);
        System.out.println("Client started and was done by Thread number "+Thread.currentThread().getId());

    }
    void addedNode(){
        ringStructure.addNode();
    }
    public String sendToPort(String input, Socket socket) throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        dataOutputStream.writeUTF(input);
        dataOutputStream.flush();
        InputStream inputStream = socket.getInputStream();
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        String message = dataInputStream.readUTF();
        System.out.println("Received from server: " +message);
        return message;
    }
    int isAnyPortAvailable(int numberOfPorts){
        int i=0;
        Random rand = new Random();
        int randomConnectToNode = rand.nextInt(numberOfPorts)+1;
        return randomConnectToNode+START_PORT;
    }

    String sendRequest(String key, String value,boolean isSetRequest ) throws IOException {
        String input;
        if (isSetRequest){
            input = "set(" + key + "," + value + ")";
        }
        else{
            input = "get(" + key + ")";
        }
        System.out.println("Client sent: " + input);
        int serverPortNumber= isAnyPortAvailable(numberOfNodes);

        try (Socket socket = new Socket("localhost", serverPortNumber)) {
            System.out.println("Client received: " + "Server port number is " + serverPortNumber);
            String answer= sendToPort(input, socket);
            System.out.println("Client received: " + answer);
            return answer;
        }catch (IOException socket){
            System.out.println("Client received: " + "Server port number is " + serverPortNumber);
            System.out.println("Client received: " + "Server is not available");
            return "Server is not available";
        }

    }
    Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
            try {
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    System.out.println("Enter the key");
                    String key = scanner.nextLine();
                    System.out.println("Enter the value");
                    String value = scanner.nextLine();
                    String answer = sendRequest(key, value, true);
                    System.out.println("Client received: " + answer);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    });

    public static void main(String[] args) {

    }

}
