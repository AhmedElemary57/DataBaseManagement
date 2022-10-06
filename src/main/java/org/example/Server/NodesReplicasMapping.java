package org.example.Server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodesReplicasMapping {
    int replicationFactor,numberOfNodes, startingPortNumber ;
    Map<Integer, List<Integer>> nodeReplicasPositionMapping;// to ask for the position of that node replicas

    Map<Integer, List<Integer>>  whichReplicasBelongToNode;//give me a map of node and its connected replicas
    List<Integer> changedNodes;

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public void setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    public int getStartingPortNumber() {
        return startingPortNumber;
    }

    public void setStartingPortNumber(int startingPortNumber) {
        this.startingPortNumber = startingPortNumber;
    }

    public Map<Integer, List<Integer>> getNodeReplicasPositionMapping() {
        return nodeReplicasPositionMapping;
    }

    public void setNodeReplicasPositionMapping(Map<Integer, List<Integer>> nodeReplicasPositionMapping) {
        this.nodeReplicasPositionMapping = nodeReplicasPositionMapping;
    }

    public Map<Integer, List<Integer>> getWhichReplicasBelongToNode() {
        return whichReplicasBelongToNode;
    }

    public void setWhichReplicasBelongToNode(Map<Integer, List<Integer>> whichReplicasBelongToNode) {
        this.whichReplicasBelongToNode = whichReplicasBelongToNode;
    }

    public List<Integer> getChangedNodes() {
        return changedNodes;
    }

    public void setChangedNodes(List<Integer> changedNodes) {
        this.changedNodes = changedNodes;
    }

    public NodesReplicasMapping(int replicationFactor, int numberOfNodes, int startingPortNumber) {
        this.replicationFactor = replicationFactor;
        this.numberOfNodes = numberOfNodes;
        this.nodeReplicasPositionMapping = new HashMap<>();
        this.whichReplicasBelongToNode = new HashMap<>();
        this.startingPortNumber = startingPortNumber;

    }
    public void addNode() {
        numberOfNodes++;
        distributeReplicas();
        generateNodeWhichReplicaBelongToNode();
        changedNodes = new ArrayList<>();
        for (int i = 0; i < replicationFactor - 1; i++) {
            changedNodes.add(startingPortNumber + i);
        }
    }
    public void printChangedNodes(){
        for (int i = 0; i < changedNodes.size(); i++) {
            System.out.println(changedNodes.get(i));
        }
    }
    public List<Integer> getPositionReplicasOfNode(int nodeID){
        List<Integer> copy = new ArrayList<>(nodeReplicasPositionMapping.get(nodeID));
        return copy;
    }

    public int getReplicationFactor(){
        return replicationFactor;
    }
    public int getNumberOfNodes(){
        return numberOfNodes;
    }

    public void print(){
        for (Map.Entry<Integer, List<Integer>> entry : nodeReplicasPositionMapping.entrySet()) {
            System.out.println("NodeID: " + entry.getKey() + " Replicas: " + entry.getValue());
        }
    }
    public void printWhichReplicasBelongToNode(){
        for (Map.Entry<Integer, List<Integer>> entry : whichReplicasBelongToNode.entrySet()) {
            System.out.println("NodeID: " + entry.getKey() + " Replicas: " + entry.getValue());
        }
    }

    public void distributeReplicas(){
        nodeReplicasPositionMapping = new HashMap<>();
        int numberOfReplicasPerNode = replicationFactor;

        for (int i = 0; i < numberOfNodes; i++) {
            List<Integer> replicaIDs = new ArrayList<>();
            for (int j = 0; j < numberOfReplicasPerNode; j++) {
                replicaIDs.add(startingPortNumber+(i+j)%numberOfNodes);
            }
            nodeReplicasPositionMapping.put(startingPortNumber+i, replicaIDs);
        }

    }
    public void generateNodeWhichReplicaBelongToNode(){
        whichReplicasBelongToNode = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : nodeReplicasPositionMapping.entrySet()) {
            int nodeID = entry.getKey();
            List<Integer> replicaIDs = entry.getValue();
            for (int i = 0; i < replicaIDs.size(); i++) {
                int replicaID = replicaIDs.get(i);
                if (whichReplicasBelongToNode.containsKey(replicaID)){
                    whichReplicasBelongToNode.get(replicaID).add(nodeID);
                }
                else{
                    List<Integer> nodeIDs = new ArrayList<>();
                    nodeIDs.add(nodeID);
                    whichReplicasBelongToNode.put(replicaID, nodeIDs);
                }
            }
        }
    }


    public static void main(String[] args) {
        NodesReplicasMapping nodesReplicasMapping = new NodesReplicasMapping(4, 8, 5001);
        nodesReplicasMapping.distributeReplicas();
        nodesReplicasMapping.print();
        nodesReplicasMapping.printWhichReplicasBelongToNode();
        nodesReplicasMapping.generateNodeWhichReplicaBelongToNode();
        System.out.println("Node which replica belong to node");
        nodesReplicasMapping.printWhichReplicasBelongToNode();
        nodesReplicasMapping.addNode();
        System.out.println("************************************************");
        System.out.println("Node which replica belong to node");
        nodesReplicasMapping.printWhichReplicasBelongToNode();
        System.out.println("************************************************");
        nodesReplicasMapping.printChangedNodes();
    }
}




