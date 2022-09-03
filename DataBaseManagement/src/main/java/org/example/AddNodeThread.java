package org.example;

import java.io.IOException;
import java.util.List;

public class AddNodeThread extends Thread{
    List<LSMTree> lsmTrees;
    RingStructure ringStructure;
    String request;
    int currentPortNumber;
    public AddNodeThread(List<LSMTree> lsmTrees, RingStructure ringStructure, String request, int currentPortNumber) {
        this.lsmTrees = lsmTrees;
        this.ringStructure = ringStructure;
        this.request = request;
        this.currentPortNumber = currentPortNumber;
    }

    @Override
    synchronized public void run() {

        try {
                for (LSMTree lsmTree : lsmTrees) {
                    lsmTree.flushToDisk();
                }
                ringStructure.addNode();
                ringStructure.nodesReplicasMapping.printWhichReplicasBelongToNode();
                ringStructure.nodesReplicasMapping.printChangedNodes();
                String newNodePort = request.split(" ")[1];
                String newPartitionPath = "/home/elemary/Projects/DataBaseManagement/Node_Number"+ newNodePort +"/ReplicaOf"+ newNodePort +"/Data/";
                String oldPartitionPath = "/home/elemary/Projects/DataBaseManagement/Node_Number"+ currentPortNumber  +"/ReplicaOf"+ currentPortNumber +"/Data/";
                Rehash.createNewPartition(newPartitionPath, oldPartitionPath);
                //TODO: send replicas to the new node
                //moveReplicas(Integer.parseInt(newNodePort));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
}
