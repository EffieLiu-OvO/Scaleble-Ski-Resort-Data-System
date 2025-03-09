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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MultithreadedClient {
    private static final int INITIAL_THREAD_COUNT = 32;
    private static final int REQUESTS_PER_THREAD = 1000;
    private static final int TOTAL_REQUESTS = 200000;
    private static final int PHASE2_THREAD_COUNT = 64; // Changed from 32 to 64
    private static final int QUEUE_CAPACITY = 10000;
    private static final int BATCH_SIZE = 500; // Added batch size constant
    private static final AtomicInteger successfulRequests = new AtomicInteger(0);
    private static final AtomicInteger failedRequests = new AtomicInteger(0);
    private static final List<RequestRecord> records = Collections.synchronizedList(new ArrayList());

    public MultithreadedClient() {
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        BlockingQueue<JsonObject> eventQueue = new LinkedBlockingQueue(QUEUE_CAPACITY);
        LiftRideEventProducer producer = new LiftRideEventProducer(eventQueue);
        Thread producerThread = new Thread(producer);
        producerThread.start();
        ExecutorService phase1Executor = Executors.newFixedThreadPool(INITIAL_THREAD_COUNT);
        SkiersClient client = new SkiersClient(records);

        for(int i = 0; i < INITIAL_THREAD_COUNT; ++i) {
            phase1Executor.submit(() -> {
                for(int j = 0; j < REQUESTS_PER_THREAD; ++j) {
                    try {
                        JsonObject liftRide = (JsonObject)eventQueue.take();
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
            });
        }

        phase1Executor.shutdown();
        while(!phase1Executor.isTerminated()) {
            Thread.yield();
        }

        int remainingRequests = TOTAL_REQUESTS - (INITIAL_THREAD_COUNT * REQUESTS_PER_THREAD);
        if (remainingRequests > 0) {
            ExecutorService phase2Executor = Executors.newFixedThreadPool(PHASE2_THREAD_COUNT);
            int numBatches = (int) Math.ceil((double) remainingRequests / BATCH_SIZE);

            for(int i = 0; i < numBatches; i++) {
                int currentBatchSize = (i == numBatches - 1) ?
                        remainingRequests - (i * BATCH_SIZE) : BATCH_SIZE;

                phase2Executor.submit(() -> {
                    for(int j = 0; j < currentBatchSize; ++j) {
                        try {
                            JsonObject liftRide = (JsonObject)eventQueue.take();
                            if (client.sendPostRequest(liftRide)) {
                                successfulRequests.incrementAndGet();
                            } else {
                                failedRequests.incrementAndGet();
                            }
                        } catch (InterruptedException var5) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                });
            }

            phase2Executor.shutdown();
            while(!phase2Executor.isTerminated()) {
                Thread.yield();
            }
        }

        producer.stop();
        try {
            producerThread.join();
        } catch (InterruptedException var15) {
            Thread.currentThread().interrupt();
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double throughput = (double)successfulRequests.get() / ((double)totalTime / (double)1000.0F);
        System.out.println("Client Configuration:");
        System.out.println("Initial Thread Count: " + INITIAL_THREAD_COUNT);
        System.out.println("Phase 2 Thread Count: " + PHASE2_THREAD_COUNT);
        System.out.println("Batch Size: " + BATCH_SIZE);
        System.out.println("\nResults:");
        System.out.println("Total successful requests: " + successfulRequests.get());
        System.out.println("Total failed requests: " + failedRequests.get());
        System.out.println("Total time: " + totalTime + " ms");
        System.out.println("Throughput: " + throughput + " requests/sec");
        System.out.println("\nPerformance Statistics:");
        calculateAndPrintStatistics();
        writeRecordsToCSV();
    }

    private static void calculateAndPrintStatistics() {
        List<Long> latencies = (List)records.stream().map(RequestRecord::getLatency).sorted().collect(Collectors.toList());
        double meanResponseTime = latencies.stream().mapToLong(Long::longValue).average().orElse((double)0.0F);
        long medianResponseTime = (Long)latencies.get(latencies.size() / 2);
        int p99Index = (int)Math.ceil((double)latencies.size() * 0.99) - 1;
        long p99ResponseTime = (Long)latencies.get(p99Index);
        long minResponseTime = (Long)latencies.get(0);
        long maxResponseTime = (Long)latencies.get(latencies.size() - 1);
        PrintStream var10000 = System.out;
        Object[] var10002 = new Object[]{meanResponseTime};
        var10000.println("Mean Response Time: " + String.format("%.2f", var10002) + " ms");
        System.out.println("Median Response Time: " + medianResponseTime + " ms");
        System.out.println("p99 Response Time: " + p99ResponseTime + " ms");
        System.out.println("Min Response Time: " + minResponseTime + " ms");
        System.out.println("Max Response Time: " + maxResponseTime + " ms");
    }

    private static void writeRecordsToCSV() {
        try (FileWriter writer = new FileWriter("request_records.csv")) {
            writer.write("StartTime,RequestType,Latency,ResponseCode\n");
            for(RequestRecord record : records) {
                writer.write(record.toCsvString() + "\n");
            }
            System.out.println("\nRequest records have been written to request_records.csv");
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
        }
    }
}