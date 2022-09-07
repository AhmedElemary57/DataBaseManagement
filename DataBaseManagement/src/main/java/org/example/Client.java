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
    public Client() throws InterruptedException {
        this.portNumber = 7999;
        System.out.println("Client started and was done by Thread number "+Thread.currentThread().getId());
        thread.start();
        thread.join();
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
        for (int i = 0; i < numberOfNodes*3; ) {
            int serverPortNumber= isAnyPortAvailable(numberOfNodes);

            try (Socket socket = new Socket("localhost", serverPortNumber)) {
                System.out.println("Server port number is " + serverPortNumber);
                String answer= sendToPort(input, socket);
                System.out.println("Client received: " + answer);
                return answer;
            }catch (IOException socket){
                System.out.println("Server "+serverPortNumber+ " is not available I will try again ");
                i++;
            }
        }
        return "Servers is not available";
   }
    Thread thread = new Thread(new Runnable() {

        @Override
        public void run() {
            try {
                ServerSocket serverSocket = new ServerSocket(portNumber);

                System.out.println("Waiting Server ....");
                Socket socket = serverSocket.accept();
                InputStream inputStream = socket.getInputStream();
                DataInputStream dataInputStream = new DataInputStream(inputStream);
                String message = dataInputStream.readUTF();
                System.out.println("Received from Admin : " + message);

                if (message.startsWith("start")){
                    numberOfNodes = Integer.parseInt(message.split(" ")[1]);
                    System.out.println("Number of nodes is " + numberOfNodes);

                } else if(message.startsWith("addNode ")){
                    numberOfNodes++;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    });

    public static void main(String[] args) throws InterruptedException {
        Client client = new Client();
        Thread.sleep(10000);
        for (int i = 0; i < 100; i++) {
            try {
                Thread.sleep(100);
                client.sendRequest("key"+i,"value"+i,true);
                client.sendRequest("key"+i,"value"+i,false);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }



    }

}