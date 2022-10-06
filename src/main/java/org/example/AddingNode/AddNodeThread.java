package org.example.AddingNode;

import org.example.LSMTree.LSMTree;
import org.example.Server.Server;

import java.io.IOException;
import java.util.List;

public class AddNodeThread extends Thread{
    List<LSMTree> lsmTrees;
    String request;
    int currentPortNumber;
    public AddNodeThread(List<LSMTree> lsmTrees, String request, int currentPortNumber) {
        this.lsmTrees = lsmTrees;
        this.request = request;
        this.currentPortNumber = currentPortNumber;
    }

    @Override
    synchronized public void run() {

        try {
                for (LSMTree lsmTree : lsmTrees) {
                    lsmTree.flushToDisk();
                }
                Server.ringStructure.addNode();
                 Server.nodesReplicasMapping.printWhichReplicasBelongToNode();
                Server.nodesReplicasMapping.printChangedNodes();
                String newNodePort = request.split(" ")[1];
                String newPartitionPath = "./Node_Number"+ newNodePort +"/ReplicaOf"+ newNodePort +"/Data/";
                for(Integer entry : Server.nodesReplicasMapping.getWhichReplicasBelongToNode().get(currentPortNumber)) {
                    String oldPartitionPath = "./Node_Number"+ currentPortNumber  +"/ReplicaOf"+ entry +"/Data/";
                    if(entry==currentPortNumber){
                        Rehash.createNewPartition(newPartitionPath, oldPartitionPath,false);
                    }
                    else{
                        Rehash.createNewPartition(newPartitionPath, oldPartitionPath,true);
                    }
                }
                //TODO: send replicas to the new node
                //moveReplicas(Integer.parseInt(newNodePort));
                 int replicaToBeRemoved = (currentPortNumber-5000)+ Server.ringStructure.getNumberOfNodes() - Server.ringStructure.replicationFactor;
                 for (LSMTree lsmTree : lsmTrees) {
                    if ( lsmTree.getReplicaId()-5000 == replicaToBeRemoved) {
                        Thread.sleep(15000);
                        System.out.println("Removing replica "+lsmTree.getReplicaId() +" and Insert new replica " + 5000+ Server.ringStructure.getNumberOfNodes());
                        lsmTree.setReplicaId(5000 + Server.ringStructure.getNumberOfNodes());
                        lsmTree.fillSegmentIDs();
                        System.out.println("Set replica "+lsmTree.getReplicaId());
                        for (int i=0; i<lsmTree.segmentIDs.size(); i++) {
                            System.out.println("Segment---- "+lsmTree.segmentIDs.get(i));
                        }

                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            Rearrange.start(Server.nodeNumber, Server.numberOfNodes, Server.replicationFactor);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        Server.sem.release();

    }
}
