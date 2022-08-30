package org.example;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.Scanner;

public class Client {

    static ServerSocket clientSocket;

     String setRequest(String key, String value) throws IOException {
         int numberOfNodes=5;
         Random rand = new Random();
         int randomConnectToNode = rand.nextInt(numberOfNodes)+1;
         Socket socket = new Socket("localhost", 5000 + randomConnectToNode);
         OutputStream outputStream = socket.getOutputStream();
         // create a data output stream from the output stream so we can send data through it
         DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
         String input = "set(" + key + "," + value + ")";
         dataOutputStream.writeUTF(input);
         dataOutputStream.flush(); // send the message

         InputStream inputStream = socket.getInputStream();
         // create a DataInputStream so we can read data from it.
         DataInputStream dataInputStream = new DataInputStream(inputStream);
         // read the message from the socket
         String message = dataInputStream.readUTF();
         System.out.println("Received from server: " +message);
         return message;
     }
     String getRequest(String key) throws IOException {
            int numberOfNodes=5;
            Random rand = new Random();
            int randomConnectToNode = rand.nextInt(numberOfNodes)+1;
            Socket socket = new Socket("localhost", 5000 + randomConnectToNode);
            OutputStream outputStream = socket.getOutputStream();
            // create a data output stream from the output stream so we can send data through it
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            String input = "get(" + key + ")";

            dataOutputStream.writeUTF(input);

            dataOutputStream.flush(); // send the message
            InputStream inputStream = socket.getInputStream();

            // create a DataInputStream so we can read data from it.
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            // read the message from the socket
            String message = dataInputStream.readUTF();
            System.out.println("Received from server: " +message);
            return message;

     }

    public static void main(String[] args) throws IOException {

        int numberOfNodes=5;
        Random rand = new Random();

        while(true) {

            int randomConnectToNode = rand.nextInt(numberOfNodes)+1;

            Socket s = new Socket("localhost", 5000 + randomConnectToNode);

            System.out.println("------------------------------------------------------------------------- ");
            OutputStream outputStream = s.getOutputStream();
            // create a data output stream from the output stream so we can send data through it
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

            System.out.println("Sending string to the ServerSocket");

            Scanner scanner = new Scanner(System.in);
            String input = scanner.nextLine();

            // write the message we want to send
            dataOutputStream.writeUTF(input);
            dataOutputStream.flush(); // send the message

            InputStream inputStream = s.getInputStream();
            // create a DataInputStream so we can read data from it.
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            // read the message from the socket
            String message = dataInputStream.readUTF();

            System.out.println("Received from server: " +message);

        }
    }


}
