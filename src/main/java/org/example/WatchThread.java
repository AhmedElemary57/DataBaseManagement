package org.example;

import java.io.IOException;
import java.nio.file.*;

public class WatchThread extends Thread{
        String dir;
        WatchEvent.Kind<Path> kind ;
        public WatchThread(String directory, WatchEvent.Kind<Path> eventKind) {
            this.dir = directory;
            this.kind = eventKind;
         }

        @Override
        public void run() {
            try {
                WatchService watchService = FileSystems.getDefault().newWatchService();

                Path path = Paths.get(dir);

                path.register(watchService, kind);

                WatchKey key;
                key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == kind) {
                        System.out.println("Event kind:" + event.kind() + ". File affected: " + event.context() + ".");
                    }
                }
                System.out.println("Done");
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
}
