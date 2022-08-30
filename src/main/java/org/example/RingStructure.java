package org.example;

import java.util.*;
import org.apache.commons.codec.digest.MurmurHash3;

public class RingStructure {
/**
 * n case of using all in all topology do the following.
 * Using replicas in the routing function
 *
 *     We have a replicas map of nodes and their replicas (port -> array of ports).
 *     Change nodes_Ports map from <Integer, Integer> to <Integer, List>
 *     Use the value of nodes_Ports map as a key in the replicas map to get the replicas ports and then appends it to the List
 * */
    int numberOfNodes, numberOfVirtualNodes, replicationFactor;
    Map<Integer,Integer> nodes_Ports = new HashMap<>();
    NodesReplicasMapping nodesReplicasMapping;

    private volatile static RingStructure uniqueInstance;
    private RingStructure(int numberOfNodes, int numberOfVirtualNodes, int replicationFactor) {
        this.numberOfNodes=numberOfNodes;
        this.numberOfVirtualNodes=numberOfVirtualNodes;
        this.replicationFactor=replicationFactor;
        nodesReplicasMapping = new NodesReplicasMapping(replicationFactor, numberOfNodes, 5001);

        nodesReplicasMapping.distributeReplicas();
        nodesReplicasMapping.generateNodeWhichReplicaBelongToNode();

    }
    public static RingStructure getInstance(int numberOfNodes, int numberOfVirtualNodes, int replicationFactor) {
        if (uniqueInstance==null){
            synchronized (RingStructure.class){
                if (uniqueInstance==null){
                    uniqueInstance= new RingStructure(numberOfNodes, numberOfVirtualNodes, replicationFactor);
                }
            }
        }
        return uniqueInstance;
    }

     int find_Node(int K) {
        // Lower and upper bounds
        int start = 0;
        int end = numberOfNodes - 1;
        // Traverse the search space
        while (start <= end) {
            int mid = (start + end) / 2;
            // If K is found
            if (keys.get(mid) == K)
                return mid;
            else if (keys.get(mid)< K)
                start = mid + 1;
            else
                end = mid - 1;
        }

        // Return insert position
        return keys.get((end + 1)%this.numberOfNodes);
    }
    //5000 get numberOf nodes then number of virtual nodes
    // fixed to 10
     List<Integer> keys= new ArrayList<>();
    void buildMap(int numberOfVirtualNodes) {
        for (int i = 1; i <= numberOfNodes; i++) {
            int amp = 414248133;
            for (int j = 0; j < numberOfVirtualNodes; j++) {
                amp += 87187;
                int portNumber = 5000 + i;
                String string = Integer.toString(amp).concat(Integer.toString(portNumber*7979));
                int hashed = MurmurHash3.hash32x86(string.getBytes());
                keys.add(hashed);
                nodes_Ports.put(hashed,portNumber);
            }
        }
        Collections.sort(keys);
    }
    void addNode(int number){
        for (int j = 0; j < numberOfVirtualNodes; j++) {
            int amp = 87187*numberOfNodes;
            numberOfNodes++;
            int portNumber = 5000 + numberOfNodes;
            String string = Integer.toString(amp).concat(Integer.toString(portNumber*7979));
            int hashed = MurmurHash3.hash32x86(string.getBytes());
            keys.add(hashed);
            nodes_Ports.put(hashed,portNumber);
        }
        Collections.sort(keys);
        nodesReplicasMapping.addNode();
        //Here we should stop the modified section in the ring structure and move the data from one node to the other

    }

    public static void main(String[] args) {
        RingStructure ringStructure = RingStructure.getInstance(5,5,3);
        ringStructure.buildMap(5);
        System.out.println(ringStructure.nodes_Ports);
        System.out.println(ringStructure.keys);
        System.out.println(ringStructure.find_Node(10));
    }

}
