package org.example.consistentHashing;

import org.example.Requests;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RingStructureDraw extends JPanel {

    static RingStructure ringStructure;
    private static final int SIZE = 10000;
    private int a = SIZE / 2;
    private int b = a;
    private int r = 4 * SIZE / 5;

    public RingStructureDraw() {
        super(true);
        this.setPreferredSize(new Dimension(SIZE, SIZE));
    }
    public void DrawPointsOnTheRing(Graphics g, int n, int r2) {
        super.paintComponent(g);
        Graphics2D g2d = (Graphics2D) g;
        g2d.setRenderingHint(
                RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON);

        long val= (long) (n+Math.pow(2,30));
        long max= (long) Math.pow(2,32);
        double t = 2 * Math.PI * val/max ;
        int x = (int) Math.round(a + r * Math.cos(t));
        int y = (int) Math.round(b + r * Math.sin(t));
        g2d.setColor(Color.black);
        g2d.drawString("----------"+String.valueOf(n),x - r2,y );
        g2d.fillOval(x - r2, y - r2,  r2/2,  r2/2);



    }
    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        Graphics2D g2d = (Graphics2D) g;
        g2d.setRenderingHint(
                RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON);
        g2d.setColor(Color.black);

        a = getWidth() / 2;
        b = getHeight() / 2;
        int m = Math.min(a, b);
        r = 4 * m / 5;
        int r2 = Math.abs(m - r) / 8;
        g2d.drawOval(a - r, b - r, 2 * r, 2 * r);
        Map<Integer,Color> map=new HashMap<>();
        Collections.sort(ringStructure.keys);

        for (int i = 1; i <= ringStructure.numberOfNodes; i++) {
            Color color = new Color((int)(Math.random() * 0x1000000));
            map.put(5000+i,color);
            g2d.setColor(color);
            g2d.drawString(String.valueOf(5000+i), 50,50+20*i);

        }

        for (int i = 0; i < ringStructure.keys.size(); i++) {
            long val= (long) (ringStructure.keys.get(i)+Math.pow(2,31));
            long max= (long) Math.pow(2,32);
            double t = 2 * Math.PI * val/max ;
            int x = (int) Math.round(a + r * Math.cos(t));
            int y = (int) Math.round(b + r * Math.sin(t));
            Color color=map.get(ringStructure.nodes_Ports.get(ringStructure.keys.get(i)));
            g2d.setColor(color);
            g2d.drawString(String.valueOf("     "+ringStructure.keys.get(i)),x,y);
            g2d.fillOval(x - r2, y - r2, 2 * r2, 2 * r2);
        }

        for (int i = 0 ; i<4 ; i++){
            long val= (long) (i*Math.pow(2,30));
            long max= (long) Math.pow(2,32);
            double t = 2 * Math.PI * val/max ;
            int x = (int) Math.round(a + r * Math.cos(t));
            int y = (int) Math.round(b + r * Math.sin(t));
            g2d.setColor(Color.black);
            g2d.drawString("----------"+String.valueOf(i),x - r2,y );
            g2d.fillOval(x - r2, y - r2, 2 * r2, 2 * r2);
        }
    }

    private static void create() {
        JFrame f = new JFrame();
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.add(new RingStructureDraw());
        f.pack();
        f.setVisible(true);
    }

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(7777);
        Socket socket = serverSocket.accept();
        String request = Requests.getInputFromSocket(socket);
        System.out.println("got "+request);
        String[] requestArray = request.split(" ");
        int numberOfNodes = Integer.parseInt(requestArray[0]);
        int numberOfVirtualNode = Integer.parseInt(requestArray[1]);
        int numberOfReplicas = Integer.parseInt(requestArray[2]);
        System.out.println("numberOfNodes "+numberOfNodes);
        System.out.println("numberOfVirtualNode "+numberOfVirtualNode);
        System.out.println("numberOfReplicas "+numberOfReplicas);

        ringStructure = RingStructure.getInstance(numberOfNodes, numberOfVirtualNode, numberOfReplicas);
        ringStructure.buildMap(numberOfVirtualNode);
        EventQueue.invokeLater(RingStructureDraw::create);
//        create();
//
//        ServerSocket serverSocketForModification = new ServerSocket(7775);
//        while (true){
//            Socket socketForModification = serverSocketForModification.accept();
//            String requestForModification = Server.getInputFromSocket(socketForModification);
//        }





    }
}