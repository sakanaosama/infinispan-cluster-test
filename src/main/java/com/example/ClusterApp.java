package com.example;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.JChannel;
import org.jgroups.stack.IpAddress;


public class ClusterApp {

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        System.out.println("Starting cluster app...");

        try (EmbeddedCacheManager cacheManager = new DefaultCacheManager("infinispan.xml")) {
            cacheManager.getCacheNames(); // Triggers cache initialization

            Cache<String, Boolean> flags = cacheManager.getCache("flags");
            Cache<String, String> workQueue = cacheManager.getCache("workQueue");

            boolean isInitializer = flags.putIfAbsent("init_done", false) == null;

            if (isInitializer) {
                System.out.println("[INIT] This node is initializing...");

                // Simulate multiple init stages with random delays
                runInitialStages();

                // Fill work queue
                for (int i = 0; i < 30; i++) {
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

            // Print cluster info
            printNodeInfo(cacheManager);

            // Now all nodes perform work
            doCalculation(cacheManager.getAddress().toString(), workQueue);
        }
    }

    private static void printNodeInfo(EmbeddedCacheManager cacheManager) {
        String clusterName = cacheManager.getClusterName();
        String nodeAddress = cacheManager.getAddress().toString();

        int port = -1;
        try {
            var transport = cacheManager.getTransport();
            if (transport instanceof JGroupsTransport jt) {
                JChannel channel = jt.getChannel();

                // Get the physical address (e.g. 10.0.0.83:64846)
                var physAddr = channel.down(new org.jgroups.Event(org.jgroups.Event.GET_PHYSICAL_ADDRESS));
                if (physAddr instanceof IpAddress ip) {
                    port = ip.getPort();
                }
            }
        } catch (Exception e) {
            System.out.println("[WARN] Failed to extract JGroups port: " + e.getMessage());
        }


        System.out.printf("[NODE] Cluster: '%s', Node: %s, Port: %s%n",
                clusterName, nodeAddress, port != -1 ? port : "unknown");
    }

    private static void runInitialStages() throws InterruptedException {
        var executor = Executors.newFixedThreadPool(4);
        var futures = new ConcurrentHashMap<String, CompletableFuture<Void>>();
        var random = new Random();

        // Stage A: no dependencies
        futures.put("A", CompletableFuture.runAsync(() -> {
            delay("A", random.nextInt(5) + 1);
        }, executor));

        // Stage B: no dependencies
        futures.put("B", CompletableFuture.runAsync(() -> {
            delay("B", random.nextInt(5) + 1);
        }, executor));

        // Stage C: depends on B
        futures.put("C", futures.get("B").thenRunAsync(() -> {
            delay("C (after B)", random.nextInt(5) + 1);
        }, executor));

        // Stage D: depends on A, B, and C
        futures.put("D", CompletableFuture.allOf(
                futures.get("A"), futures.get("B"), futures.get("C")
        ).thenRunAsync(() -> {
            delay("D (after A, B, C)", random.nextInt(5) + 1);
        }, executor));

        // Wait for all stages to finish
        CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).join();
        executor.shutdown();
    }

    private static void delay(String a, int i) {
        int delay = RANDOM.nextInt(i) + 1; // Random delay between 1–i seconds
        System.out.println("[INIT] " + a + " ... (delay " + delay + "s)");
        try {
            TimeUnit.SECONDS.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("[INIT] " + a + " done.");
    }

    private static void doCalculation(String nodeId, Cache<String, String> workQueue) throws InterruptedException {
        System.out.println("[WORK] " + nodeId + " starting to process work...");

        while (!workQueue.isEmpty()) {
            for (String key : workQueue.keySet()) {
                String task = workQueue.remove(key);
                if (task != null) {
                    System.out.println("[WORK] " + nodeId + " processing: " + task);
                    TimeUnit.MILLISECONDS.sleep(3000);
                }
            }
        }

        System.out.println("[WORK] " + nodeId + " has no more work.");
    }
}
