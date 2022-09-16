package org.example.LSMTree;

import org.example.Server.Server;

import java.io.IOException;

public class CompactionThread extends Thread{
    LSMTree lsmTree;
    public CompactionThread(LSMTree lsmTree) {
        this.lsmTree = lsmTree;
    }


    @Override
    synchronized public void run() {

            System.out.println("CompactionThread started for replica "+ lsmTree.replicaId + " and was done by Thread number "+Thread.currentThread().getId());
            try {
                if (lsmTree.segmentIDs.size() > 2) {
                    lsmTree.mergeCompaction();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println("CompactionThread finished for replica "+ lsmTree.replicaId + " and was done by Thread number "+Thread.currentThread().getId());
            Server.sem.release();
        }
}
