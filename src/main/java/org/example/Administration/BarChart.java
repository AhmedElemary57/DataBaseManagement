package org.example.Administration;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.io.File;
import java.util.*;

import javax.swing.JFrame;
import javax.swing.JPanel;


public class BarChart extends JPanel
{
    static Color[] colors ={Color.RED, Color.BLUE, Color.GREEN, Color.YELLOW,
            Color.MAGENTA, Color.CYAN, Color.PINK,
            Color.LIGHT_GRAY, Color.DARK_GRAY,Color.ORANGE,Color.GRAY,Color.BLACK,Color.WHITE
            ,Color.decode("#FFA500"),Color.decode("#FF0000"),Color.decode("#FF00FF"),Color.decode("#FFFF00"),Color.decode("#00FF00"),Color.decode("#00FFFF"),Color.decode("#0000FF")};
    static int counter=0;

    private Map<Color, Integer> bars =
            new LinkedHashMap<Color, Integer>();

    /**
     * Add new bar to chart
     * @param color color to display bar
     * @param value size of bar
     */
    public void addBar(Color color, int value)
    {
        bars.put(color, value);
        repaint();
    }

    @Override
    protected void paintComponent(Graphics g)
    {
        // determine longest bar

        int max = Integer.MIN_VALUE;
        for (Integer value : bars.values())
        {
            max = Math.max(max, value);
        }

        // paint bars

        int width = (getWidth() / bars.size()) - 2;
        int x = 1;

        for (Color color : bars.keySet())
        {
            int value = bars.get(color);
            int height = (int)
                    ((getHeight()-30) * ((double)value / max));
            g.setColor(color);
            g.fillRect(x, getHeight() - height, width, height);
            g.setColor(Color.black);
            g.drawRect(x, getHeight() - height, width, height);
            g.drawString(String.valueOf(value/1000000.0)+"MB", x+2, getHeight() - 5);
            x += (width + 2);
        }
    }
    public static long folderSize(File directory) {
        long length = 0;
        if (directory.exists()) {
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                if (file.isFile())
                    length += file.length();
                else
                    length += folderSize(file);
            }
        }
        return length;
    }
    public static long getPartitionSize(int nodeNumber){
        int portNumber = 5000 + nodeNumber;
        String diskPath= "./Node_Number"+ portNumber+"/ReplicaOf"+portNumber+"/";
        File folder = new File(diskPath+"Data/");
        return  folderSize(folder);
    }
    @Override
    public Dimension getPreferredSize() {
        return new Dimension(bars.size() * 10 + 2, 50);
    }

    static JFrame frame = new JFrame("Bar Chart");

    public static void showBarChart(int numberOfNodes)
    {
        frame.setPreferredSize(new Dimension(1000, 500));
        BarChart chart = new BarChart();
        for (int i=1; i <= numberOfNodes; i++) {
            chart.addBar(colors[(i-1)%colors.length], (int) getPartitionSize(i));
        }
        frame.repaint();
        frame.getContentPane().add(chart);
        frame.pack();
    }



    //run this function each 1 second
    public static void  refresh(){

        Timer t = new Timer();
        TimerTask tt = new TimerTask() {
            @Override
            public void run() {
                frame.getContentPane().removeAll();
                showBarChart(Admin.count);

            }
        };
        if (counter%2==1){
            System.out.println("graph closed");
            tt.cancel();
            t.cancel();
            t.purge();
            frame.setVisible(false);
            counter++;
        }else{
            System.out.println("graph displayed");
            counter++;
            t.schedule(tt,0,200);
            frame.setVisible(true);

        }
    }

}

