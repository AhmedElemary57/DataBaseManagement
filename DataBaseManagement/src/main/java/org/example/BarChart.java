package org.example;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.swing.JFrame;
import javax.swing.JPanel;


public class BarChart extends JPanel
{
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
                    ((getHeight()-5) * ((double)value / max));
            g.setColor(color);
            g.fillRect(x, getHeight() - height, width, height);
            g.setColor(Color.black);
            g.drawRect(x, getHeight() - height, width, height);
            x += (width + 2);
        }
    }
    public static long folderSize(File directory) {
        long length = 0;
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            if (file.isFile())
                length += file.length();
            else
                length += folderSize(file);
        }
        return length;
    }
    public static long getPartitionSize(int nodeNumber){
        int portNumber = 5000 + nodeNumber;
        String diskPath= "./Node_Number"+ portNumber+"/ReplicaOf"+portNumber+"/";
        System.out.println(diskPath);
        File folder = new File(diskPath+"Data/");
        return  folderSize(folder);
    }
    @Override
    public Dimension getPreferredSize() {
        return new Dimension(bars.size() * 10 + 2, 50);
    }
    static Color[] colors ={Color.RED, Color.BLUE, Color.GREEN, Color.YELLOW,
            Color.MAGENTA, Color.CYAN, Color.PINK,
            Color.LIGHT_GRAY, Color.DARK_GRAY};
    static JFrame frame = new JFrame("Bar Chart");

    public static void showBarChart(int numberOfNodes)
    {
        frame.setPreferredSize(new Dimension(500, 500));
        BarChart chart = new BarChart();
        for (int i=1; i <= numberOfNodes; i++) {
            chart.addBar(colors[i-1], (int) getPartitionSize(i));
        }
        frame.getContentPane().add(chart);
        frame.pack();
        frame.setVisible(true);
    }

    public static void  refresh(int numberOfNodes){
        frame.setVisible(false);
        showBarChart(numberOfNodes);
        frame.setVisible(true);


    }
}

