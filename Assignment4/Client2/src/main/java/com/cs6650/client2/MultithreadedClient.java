package com.cs6650.client2;

import com.google.gson.JsonObject;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MultithreadedClient {
    private static final int INITIAL_THREAD_COUNT = 32;
    private static final int REQUESTS_PER_THREAD = 1000;
    private static final int TOTAL_REQUESTS = 200000;
    private static final int PHASE2_THREAD_COUNT = 248;
    private static final int QUEUE_CAPACITY = 50000;
    private static final int BATCH_SIZE = 1000;
    private static final AtomicInteger successfulRequests = new AtomicInteger(0);
    private static final AtomicInteger failedRequests = new AtomicInteger(0);
    private static final AtomicBoolean phase2Started = new AtomicBoolean(false);
    private static final List<RequestRecord> records = Collections.synchronizedList(new ArrayList<>());

    public MultithreadedClient() {
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        BlockingQueue<JsonObject> eventQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        int producerCount = 3;
        List<Thread> producerThreads = new ArrayList<>();
        List<LiftRideEventProducer> producers = new ArrayList<>();

        System.out.println("Starting producers...");
        for (int i = 0; i < producerCount; i++) {
            LiftRideEventProducer producer = new LiftRideEventProducer(eventQueue);
            Thread producerThread = new Thread(producer);
            producerThread.start();
            producerThreads.add(producerThread);
            producers.add(producer);
        }

        SkiersClient client = new SkiersClient(records);


        ExecutorService phase1Executor = Executors.newFixedThreadPool(INITIAL_THREAD_COUNT);
        ExecutorService phase2Executor = Executors.newFixedThreadPool(PHASE2_THREAD_COUNT);

        System.out.println("Starting Phase 1 threads...");
        for (int i = 0; i < INITIAL_THREAD_COUNT; ++i) {
            phase1Executor.submit(() -> {
                for (int j = 0; j < REQUESTS_PER_THREAD; ++j) {
                    try {
                        JsonObject liftRide = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (liftRide == null) {
                            System.out.println("[Phase1] No event found in queue, skipping...");
                            continue;}

                        if (client.sendPostRequest(liftRide)) {
                            successfulRequests.incrementAndGet();
                        } else {
                            failedRequests.incrementAndGet();
                        }
                    } catch (InterruptedException var4) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                if (phase2Started.compareAndSet(false, true)) {
                    System.out.println("Triggering Phase 2...");
                    startPhase2(phase2Executor, eventQueue, client);
                }
            });
        }

        phase1Executor.shutdown();

        System.out.println("Waiting for requests to complete...");
        while (successfulRequests.get() + failedRequests.get() < TOTAL_REQUESTS) {
            try {
                System.out.println("Progress: " + (successfulRequests.get() + failedRequests.get()) + "/" + TOTAL_REQUESTS);
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        for (LiftRideEventProducer producer : producers) {
            producer.stop();
        }

        for (Thread thread : producerThreads) {
            try {
                thread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        System.out.println("Shutting down Phase 2...");
        phase2Executor.shutdownNow();
        try {
            phase2Executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double throughput = (double) successfulRequests.get() / ((double) totalTime / 1000.0);

        System.out.println("Client Configuration:");
        System.out.println("Initial Thread Count: " + INITIAL_THREAD_COUNT);
        System.out.println("Phase 2 Thread Count: " + PHASE2_THREAD_COUNT);
        System.out.println("Batch Size: " + BATCH_SIZE);
        System.out.println("Producer Count: " + producerCount);
        System.out.println("Queue Capacity: " + QUEUE_CAPACITY);
        System.out.println("\nResults:");
        System.out.println("Total successful requests: " + successfulRequests.get());
        System.out.println("Total failed requests: " + failedRequests.get());
        System.out.println("Total time: " + totalTime + " ms");
        System.out.println("Throughput: " + throughput + " requests/sec");
        System.out.println("\nPerformance Statistics:");
        calculateAndPrintStatistics();
        writeRecordsToCSV();
    }

    private static void startPhase2(ExecutorService executor, BlockingQueue<JsonObject> queue, SkiersClient client) {
        System.out.println("Starting phase 2...");

        int completedRequests = successfulRequests.get() + failedRequests.get();
        int remainingRequests = TOTAL_REQUESTS - completedRequests;

        if (remainingRequests <= 0) {
            return;
        }

        int numBatches = (int) Math.ceil((double) remainingRequests / BATCH_SIZE);

        for (int i = 0; i < numBatches; i++) {
            int currentBatchSize = (i == numBatches - 1) ?
                    remainingRequests - (i * BATCH_SIZE) : BATCH_SIZE;

            executor.submit(() -> {
                for (int j = 0; j < currentBatchSize; ++j) {
                    try {
                        JsonObject liftRide = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (liftRide == null) continue;

                        if (client.sendPostRequest(liftRide)) {
                            successfulRequests.incrementAndGet();
                        } else {
                            failedRequests.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }
    }

    private static void calculateAndPrintStatistics() {
        List<Long> latencies = records.stream().map(RequestRecord::getLatency).sorted().collect(Collectors.toList());

        if (latencies.isEmpty()) {
            System.out.println("No latency data available.");
            return;
        }

        double meanResponseTime = latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long medianResponseTime = latencies.get(latencies.size() / 2);
        int p99Index = (int) Math.ceil(latencies.size() * 0.99) - 1;
        long p99ResponseTime = latencies.get(p99Index);
        long minResponseTime = latencies.get(0);
        long maxResponseTime = latencies.get(latencies.size() - 1);

        System.out.println("Mean Response Time: " + String.format("%.2f", meanResponseTime) + " ms");
        System.out.println("Median Response Time: " + medianResponseTime + " ms");
        System.out.println("p99 Response Time: " + p99ResponseTime + " ms");
        System.out.println("Min Response Time: " + minResponseTime + " ms");
        System.out.println("Max Response Time: " + maxResponseTime + " ms");
    }

    private static void writeRecordsToCSV() {
        try (FileWriter writer = new FileWriter("request_records.csv")) {
            writer.write("StartTime,RequestType,Latency,ResponseCode\n");
            for (RequestRecord record : records) {
                writer.write(record.toCsvString() + "\n");
            }
            System.out.println("\nRequest records have been written to request_records.csv");
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
        }
    }
}