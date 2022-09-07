package org.example;

import java.io.*;
import java.net.Socket;

public class Requests {
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

}
