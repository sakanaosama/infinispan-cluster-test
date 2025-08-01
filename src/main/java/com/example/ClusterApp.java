package com.example;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ClusterApp {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting cluster app...");

        try (EmbeddedCacheManager cacheManager = new DefaultCacheManager("infinispan.xml")) {
            cacheManager.getCacheNames(); // Triggers cache initialization

            Cache<String, Boolean> flags = cacheManager.getCache("flags");
            Cache<String, String> workQueue = cacheManager.getCache("workQueue");

            boolean isInitializer = flags.putIfAbsent("init_done", false) == null;

            if (isInitializer) {
                System.out.println("[INIT] This node is initializing...");

                // Simulate multiple init stages
                runInitialStage("Stage 1: DB setup", 2);
                runInitialStage("Stage 2: Preload data", 3);
                runInitialStage("Stage 3: Create work queue", 2);

                // Fill work queue
                for (int i = 0; i < 10; i++) {
                    String id = UUID.randomUUID().toString();
                    workQueue.put(id, "Work Item " + i);
                }

                flags.put("init_done", true);
                System.out.println("[INIT] Initialization complete!");
            } else {
                System.out.println("[WAIT] Initialization in progress...");

                // Wait for init to complete
                while (!Boolean.TRUE.equals(flags.get("init_done"))) {
                    TimeUnit.SECONDS.sleep(1);
                }

                System.out.println("[JOINED] Initialization confirmed.");
            }

            // Now all nodes perform work
            doCalculation(cacheManager.getAddress().toString(), workQueue);
        }
    }

    private static void runInitialStage(String stageName, int seconds) throws InterruptedException {
        System.out.println("[INIT] " + stageName + " ...");
        TimeUnit.SECONDS.sleep(seconds);
        System.out.println("[INIT] " + stageName + " done.");
    }

    private static void doCalculation(String nodeId, Cache<String, String> workQueue) throws InterruptedException {
        System.out.println("[WORK] " + nodeId + " starting to process work...");

        while (!workQueue.isEmpty()) {
            for (String key : workQueue.keySet()) {
                String task = workQueue.remove(key);
                if (task != null) {
                    System.out.println("[WORK] " + nodeId + " processing: " + task);
                    TimeUnit.MILLISECONDS.sleep(500);
                }
            }
        }

        System.out.println("[WORK] " + nodeId + " has no more work.");
    }
}
