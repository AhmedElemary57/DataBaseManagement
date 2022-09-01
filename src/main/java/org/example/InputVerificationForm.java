package org.example;

import com.formdev.flatlaf.FlatLightLaf;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.io.IOException;
import java.text.*;
import java.util.ArrayList;
import java.util.List;
public class InputVerificationForm extends JPanel {
    //Default values
    private static final int DEFAULT_NODES = 5;
    private static final int DEFAULT_V_NODES = 20;
    private static final int DEFAULT_REPLICATION = 3;
    private static final int DEFAULT_MEMTABLE = 10;
    private static final int DEFAULT_SEGMENT = 10;
    private static final int DEFAULT_WRITE_QUORUM = 2;
    private static final int DEFAULT_READ_QUORUM = 2;


    //Strings for the labels
    private static final String nodesString = "Number of Nodes:";
    private static final String vnString = "Virtual Nodes:";
    private static final String replicaString = "Replication Factor:";
    private static final String segmentString = "Segment Size:";
    private static final String memTableString = "Max MemTable Size:";
    private static final String writeQuorumString = "Write Quorum:";
    private static final String readQuorumString = "Read Quorum:";
    private static final String ratioString = "Ratio:";

    //Text fields for data entry
    public static JTextField nodesField;
    public static JTextField vnField;
    public static JTextField replicaField;
    public static JTextField segmentField;
    public static JTextField memTableField;
    public static JTextField writeQuorumField;
    public static JTextField readQuorumField;
    public static JTextField ratioField;

    //Formats to format and parse numbers

    public static final DecimalFormat formatter = new DecimalFormat("#0");

    public InputVerificationForm() {
        super(new BorderLayout());

        double consistency = computeConsistency(DEFAULT_NODES, DEFAULT_V_NODES, DEFAULT_REPLICATION);

        //Create the labels.
        //Labels to identify the text fields
        JLabel nodeLabel = new JLabel(nodesString);
        JLabel vnLabel = new JLabel(vnString);
        JLabel replicaLabel = new JLabel(replicaString);
        JLabel segmentLabel = new JLabel(segmentString);
        JLabel memTableLabel = new JLabel(memTableString);
        JLabel writeQuorumLabel = new JLabel(writeQuorumString);
        JLabel readQuorumLabel = new JLabel(readQuorumString);
        JLabel ratioLabel = new JLabel(ratioString);

        //Create the text fields and set them up.
        nodesField = new JTextField(formatter.format(DEFAULT_NODES), 10);
        MyVerifier verifier = new MyVerifier();
        nodesField.setInputVerifier(verifier);

        vnField = new JTextField(formatter.format(DEFAULT_V_NODES), 10);
        vnField.setInputVerifier(verifier);

        replicaField = new JTextField(formatter.format(DEFAULT_REPLICATION), 10);
        replicaField.setInputVerifier(verifier);

        segmentField = new JTextField(formatter.format(DEFAULT_SEGMENT), 10);
        segmentField.setInputVerifier(verifier);

        memTableField = new JTextField(formatter.format(DEFAULT_MEMTABLE), 10);
        memTableField.setInputVerifier(verifier);

        writeQuorumField = new JTextField(formatter.format(DEFAULT_WRITE_QUORUM), 10);
        writeQuorumField.setInputVerifier(verifier);

        readQuorumField = new JTextField(formatter.format(DEFAULT_READ_QUORUM), 10);
        readQuorumField.setInputVerifier(verifier);

        ratioField = new JTextField(formatter.format(consistency), 10);
        ratioField.setInputVerifier(verifier);
        ratioField.setEditable(false);
        //Remove this component from the focus cycle.
        ratioField.setFocusable(false);
        ratioField.setForeground(Color.red);

        //Register an action listener to handle Return.
        nodesField.addActionListener(verifier);
        vnField.addActionListener(verifier);
        replicaField.addActionListener(verifier);
        segmentField.addActionListener(verifier);
        memTableField.addActionListener(verifier);
        writeQuorumField.addActionListener(verifier);
        readQuorumField.addActionListener(verifier);

        //Tell accessibility tools about label/textfield pairs.
        nodeLabel.setLabelFor(nodesField);
        vnLabel.setLabelFor(replicaField);
        replicaLabel.setLabelFor(vnField);
        segmentLabel.setLabelFor(segmentField);
        memTableLabel.setLabelFor(memTableField);
        writeQuorumLabel.setLabelFor(writeQuorumField);
        readQuorumLabel.setLabelFor(readQuorumField);
        ratioLabel.setLabelFor(ratioField);

        //Lay out the labels in a panel.
        JPanel labelPane = new JPanel(new GridLayout(0,1));
        labelPane.add(nodeLabel);
        labelPane.add(vnLabel);
        labelPane.add(replicaLabel);
        labelPane.add(segmentLabel);
        labelPane.add(memTableLabel);
        labelPane.add(writeQuorumLabel);
        labelPane.add(readQuorumLabel);
        labelPane.add(ratioLabel);

        //Layout the text fields in a panel.
        JPanel fieldPane = new JPanel(new GridLayout(0,1));
        fieldPane.add(nodesField);
        fieldPane.add(vnField);
        fieldPane.add(replicaField);
        fieldPane.add(segmentField);
        fieldPane.add(memTableField);
        fieldPane.add(writeQuorumField);
        fieldPane.add(readQuorumField);
        fieldPane.add(ratioField);

        //Put the panels in this panel, labels on left,
        //text fields on right.
        setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));
        add(labelPane, BorderLayout.CENTER);
        add(fieldPane, BorderLayout.LINE_END);
    }

    @SuppressWarnings("deprecation")
    class MyVerifier extends InputVerifier implements ActionListener {
        public boolean shouldYieldFocus(JComponent input) {
            boolean inputOK = verify(input);
            makeItPretty(input);
            updateResults();

            if (inputOK) {
                return true;
            } else {
                Toolkit.getDefaultToolkit().beep();
                return false;
            }
        }

        protected void updateResults() {
            int nodes = DEFAULT_NODES;
            int vn = DEFAULT_V_NODES;
            int replicas = DEFAULT_REPLICATION;
            int segment = DEFAULT_SEGMENT;
            int memTable = DEFAULT_MEMTABLE;
            int writeQuorum = DEFAULT_WRITE_QUORUM;
            int readQuorum = DEFAULT_READ_QUORUM;

            double ratio;

            //Parse the values.
            try {
                nodes = formatter.parse(nodesField.getText()).intValue();
                vn = formatter.parse(vnField.getText()).intValue();
                replicas = formatter.parse(replicaField.getText()).intValue();
                segment = formatter.parse(segmentField.getText()).intValue();
                memTable = formatter.parse(memTableField.getText()).intValue();
                writeQuorum = formatter.parse(writeQuorumField.getText()).intValue();
                readQuorum = formatter.parse(readQuorumField.getText()).intValue();
            } catch (ParseException pe) {
                pe.printStackTrace();
            }
            //Calculate the result and update the GUI.
            ratio = computeConsistency(nodes, vn, replicas);
            ratioField.setText(formatter.format(ratio));
        }

        //This method checks input, but should cause no side effects.
        public boolean verify(JComponent input) {
            return checkField(input, false);
        }

        protected void makeItPretty(JComponent input) {
            checkField(input, true);
        }

        protected boolean checkField(JComponent input, boolean changeIt) {
            if (input == nodesField) {
                return checkNodesField(changeIt);
            } else if (input == vnField) {
                return checkVNField(changeIt);
            } else if (input == replicaField) {
                return checkReplicasField(changeIt);
            } else if (input == segmentField) {
                return checkSegmentField(changeIt);
            } else if (input == memTableField) {
                return checkMemTableField(changeIt);
            } else if (input == writeQuorumField) {
                return checkWriteQuorumField(changeIt);
            } else if (input == readQuorumField) {
                return checkReadQuorumField(changeIt);
            } else {
                return false;
            }
        }
        protected boolean checkNodesField(boolean change) {
            boolean wasValid = true;
            int nodes = DEFAULT_NODES;
            //Parse the value.
            try {
                nodes = formatter.parse(nodesField.getText()).intValue();
            } catch (ParseException pe) {
                wasValid = false;
            }
            //Value was invalid.
            if (nodes <= 0) {
                wasValid = false;
                if (change) {
                    nodes = 1;
                }
            }
            //Whether value was valid or not, format it nicely.
            if (change) {
                nodesField.setText(formatter.format(nodes));
                nodesField.selectAll();
            }
            return wasValid;
        }
        protected boolean checkVNField(boolean change) {
            boolean wasValid = true;
            double vNodes = DEFAULT_V_NODES;

            //Parse the value.
            try {
                vNodes = formatter.parse(vnField.getText()).doubleValue();
            } catch (ParseException pe) {
                wasValid = false;
                pe.printStackTrace();
            }

            //Value was invalid.
            if (vNodes < 0) {
                wasValid = false;
                if (change) {
                    vNodes = 0;
                }
            }

            //Whether value was valid or not, format it nicely.
            if (change) {
                vnField.setText(formatter.format(vNodes));
                vnField.selectAll();
            }

            return wasValid;
        }
        protected boolean checkReplicasField(boolean change) {
            boolean wasValid = true;
            int replica = DEFAULT_REPLICATION;
            int nodes = DEFAULT_NODES;
            //Parse the value.
            try {
                replica = formatter.parse(replicaField.getText()).intValue();
            } catch (ParseException pe) {
                wasValid = false;
                pe.printStackTrace();
            }

            try {
                nodes = formatter.parse(nodesField.getText()).intValue();
            } catch (ParseException pe) {
                pe.printStackTrace();
            }

            //Value was invalid.
            if ((replica <= 0) || (replica > nodes)) {
                wasValid = false;
                if (change) {
                    if (replica <= 0) {
                        replica = 1;
                    } else {
                        replica = nodes;
                    }
                }
            }

            //Whether value was valid or not, format it nicely.
            if (change) {
                replicaField.setText(formatter.format(replica));
                replicaField.selectAll();
            }

            return wasValid;
        }
        protected boolean checkSegmentField(boolean change) {
            boolean wasValid = true;
            int segment = DEFAULT_SEGMENT;
            //Parse the value.
            try {
                segment = formatter.parse(segmentField.getText()).intValue();
            } catch (ParseException pe) {
                wasValid = false;
                pe.printStackTrace();
            }
            //Value was invalid.
            if (segment < 1) {
                wasValid = false;
                if (change) {
                    segment = 1;
                }
            }
            //Whether value was valid or not, format it nicely.
            if (change) {
                segmentField.setText(formatter.format(segment));
                segmentField.selectAll();
            }
            return wasValid;
        }
        protected boolean checkMemTableField(boolean change) {
            boolean wasValid = true;
            int memTable = DEFAULT_MEMTABLE;
            //Parse the value.
            try {
                memTable = formatter.parse(memTableField.getText()).intValue();
            } catch (ParseException pe) {
                wasValid = false;
                pe.printStackTrace();
            }
            //Value was invalid.
            if (memTable < 1) {
                wasValid = false;
                if (change) {
                    memTable = 1;
                }
            }
            //Whether value was valid or not, format it nicely.
            if (change) {
                memTableField.setText(formatter.format(memTable));
                memTableField.selectAll();
            }
            return wasValid;
        }
        protected boolean checkWriteQuorumField(boolean change) {
            boolean wasValid = true;
            int writeQuorum = DEFAULT_WRITE_QUORUM;
            int replication = DEFAULT_REPLICATION;
            //Parse the value.
            try {
                writeQuorum = formatter.parse(writeQuorumField.getText()).intValue();
            } catch (ParseException pe) {
                wasValid = false;
                pe.printStackTrace();
            }
            try {
                replication = formatter.parse(replicaField.getText()).intValue();
            } catch (ParseException pe) {
                pe.printStackTrace();
            }

            //Value was invalid.
            if (writeQuorum <= 0 || writeQuorum > replication) {
                wasValid = false;
                if (change) {
                    if (writeQuorum <= 0) {
                        writeQuorum = 1;
                    } else {
                        writeQuorum = replication;
                    }
                }
            }
            //Whether value was valid or not, format it nicely.
            if (change) {
                writeQuorumField.setText(formatter.format(writeQuorum));
                writeQuorumField.selectAll();
            }
            return wasValid;
        }
        protected boolean checkReadQuorumField(boolean change) {
            boolean wasValid = true;
            int readQuorum = DEFAULT_READ_QUORUM;
            int replication = DEFAULT_REPLICATION;
            //Parse the value.
            try {
                readQuorum = formatter.parse(readQuorumField.getText()).intValue();
            } catch (ParseException pe) {
                wasValid = false;
                pe.printStackTrace();
            }
            try {
                replication = formatter.parse(replicaField.getText()).intValue();
            } catch (ParseException pe) {
                pe.printStackTrace();
            }
            //Value was invalid.
            if (readQuorum <= 0 || readQuorum > replication) {
                wasValid = false;
                if (change) {
                    if (readQuorum <= 0) {
                        readQuorum = 1;
                    } else {
                        readQuorum = replication;
                    }
                }
            }
            //Whether value was valid or not, format it nicely.
            if (change) {
                readQuorumField.setText(formatter.format(readQuorum));
                readQuorumField.selectAll();
            }
            return wasValid;
        }

        public void actionPerformed(ActionEvent e) {
            JTextField source = (JTextField)e.getSource();
            shouldYieldFocus(source); //ignore return value
            source.selectAll();
        }
    }

    public static void main(String[] args) {
        FlatLightLaf.setup();
        try {
            UIManager.setLookAndFeel("com.formdev.flatlaf.intellijthemes.FlatDarkPurpleIJTheme");
        } catch (UnsupportedLookAndFeelException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            ex.printStackTrace();
        }
        //Create and set up the window.
        JFrame frame = new JFrame("Admin");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 600);
        //Create and set up the content pane.
        JComponent newContentPane = new InputVerificationForm();
        newContentPane.setOpaque(true); //content panes must be opaque

        // add start button
        JButton startButton = new JButton("Start");
        JButton addButton = new JButton("Add Node");
        addButton.setEnabled(false);
        //startButton.addActionListener( e -> storeFields() );
        startButton.addActionListener( e -> {
            try {
                Admin.startServers(getFields());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            startButton.setEnabled(false);
            addButton.setEnabled(true);
        } );


        // make a panel for the buttons
        JPanel buttonPanel = new JPanel();
        buttonPanel.add(startButton);
        buttonPanel.add(addButton);
        newContentPane.add(buttonPanel, BorderLayout.SOUTH);

        addButton.addActionListener(e -> {
            try {
                Admin.addNode(getFields());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
        frame.setContentPane(newContentPane);

        //Display the window.
        frame.pack();
        frame.setVisible(true);
    }
    double computeConsistency(double replicas, double read, int write) {
        return (replicas + 1) / 2 * (read + write);
    }

     public static List<Integer> getFields() throws IOException {

        // create a list of the fields
        List<Integer> fields = new ArrayList<>();
        try {
            fields.add(formatter.parse(nodesField.getText()).intValue());
            fields.add(formatter.parse(vnField.getText()).intValue());
            fields.add(formatter.parse(replicaField.getText()).intValue());
            fields.add(formatter.parse(segmentField.getText()).intValue());
            fields.add(formatter.parse(memTableField.getText()).intValue());
            fields.add(formatter.parse(writeQuorumField.getText()).intValue());
            fields.add(formatter.parse(readQuorumField.getText()).intValue());
        } catch (ParseException pe) {
            pe.printStackTrace();
        }
        return fields;
    }
}