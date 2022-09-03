package org.example;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

public class ClientGUI extends JFrame {
    private JPanel mainPanel;
    private JTextField setKet;
    private JTextField setValue;
    private JButton setButton;
    private JTextField getValue;
    private JButton getButton;
    private JTextArea out;
    private JLabel yourOutputLabel;
    Client client;


    public ClientGUI()  {
        super("Client");
        client = new Client(5000,5);

        this.setButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (setKet.getText().isEmpty() || setValue.getText().isEmpty()) {
                    out.setText("Please enter a key and a value");
                } else {
                    try {
                        String response=client.sendRequest(setKet.getText(), setValue.getText(), true);
                        out.append(response+"\n");
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        });
        this.getButton.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
                if (getValue.getText().isEmpty()) {
                    out.setText("Please enter a key");
                } else {
                    try {
                        String response=client.sendRequest(getValue.getText(), "", false);
                        out.append(response+"\n");
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        });


    }

    private void createUIComponents() {


        // TODO: place custom component creation code here
    }

    public static void main(String[] args) {

        JFrame frame = new ClientGUI();
        frame.setPreferredSize(new Dimension(600, 600));
        frame.setContentPane(new ClientGUI().mainPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);

    }
}
