package org.example;

import java.util.*;

import com.cloudimpl.outstack.common.Pair;
import org.apache.commons.codec.digest.MurmurHash3;
public class RingStructure {
    public static List<Long> addedNode;
    int numberOfNodes, numberOfVirtualNodes, replicationFactor;
    Map<Long,Integer> nodes_Ports = new HashMap<>();
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

    //find the node which has the replica

     long find_Node(long K) {
        // Lower and upper bounds
        int start = 0;
        int end = keys.size()-1;
        // Traverse the search space
        while (start <= end) {
            int mid = start+(end-start)/ 2;
            // If K is found
            if (keys.get(mid) == K)
                return mid;
            else if (keys.get(mid)< K)
                start = mid + 1;
            else
                end = mid - 1;
        }
        // Return insert position
        return keys.get((end + 1));
    }
    //5000 get numberOf nodes then number of virtual nodes
    // fixed to 10
    static List<Long> keys = new ArrayList<>();
    void buildMap(int numberOfVirtualNodes) {
        final char startingSymbol = 'a';
        for (int i = 1; i <= numberOfNodes; i++) {
            char prefix = (char)((int)startingSymbol + i);
            for (int j = 0; j < numberOfVirtualNodes; j++) {
                String postfix = String.valueOf(j);
                int portNumber = 5000 + i;
                String vnName = prefix + postfix;
                long hashed = MurmurHash3.hash32x86(vnName.getBytes());
                keys.add(hashed);
                nodes_Ports.put(hashed, portNumber);
            }
        }
        Collections.sort(keys);
    }
    void addNode(){
        addedNode = new ArrayList<>();
        for (int j = 0; j < numberOfVirtualNodes; j++) {
            int amp = 87187 * numberOfNodes;
            numberOfNodes++;
            int portNumber = 5000 + numberOfNodes;
            String string = Integer.toString(amp).concat(Integer.toString(portNumber*7979));
            long hashed = MurmurHash3.hash32x86(string.getBytes());
            keys.add(hashed);
            addedNode.add(hashed);
            nodes_Ports.put(hashed,portNumber);
        }
        Collections.sort(keys);
        Collections.sort(addedNode);
        nodesReplicasMapping.addNode();
        //Here we should stop the modified section in the ring structure and move the data from one node to the other
    }

    public static void main(String[] args) {
        RingStructure ns = new RingStructure(5, 20, 3);
        ns.buildMap(20);
        System.out.println(ns.keys);
        for(Map.Entry<Long,Integer> entry : ns.nodes_Ports.entrySet()){
            System.out.println(entry.getKey() + " " + entry.getValue());
        }


    }

    public static ArrayList<ArrayList<String>> ranges() {
        ArrayList<ArrayList<String>> ranges = new ArrayList<>();
        for (Long end : addedNode) {
            int inx = keys.indexOf(end);
            if (inx == 0) {
                inx = keys.size() - 1;
            } else {
                inx = inx - 1;
            }
            Long start = keys.get(inx);
            ranges.add(new ArrayList<>(Arrays.asList(start.toString(), end.toString())));
        }
        return ranges;
    }
    public int getNumberOfNodes() {
        return this.numberOfNodes;
    }
}
