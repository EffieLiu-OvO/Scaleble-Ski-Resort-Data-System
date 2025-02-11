package com.cs6650.client2;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Collections;
import com.google.gson.JsonObject;

public class MultithreadedClient {
    private static final int INITIAL_THREAD_COUNT = 32;
    private static final int REQUESTS_PER_THREAD = 1000;
    private static final int TOTAL_REQUESTS = 200000;
    private static final int PHASE2_THREAD_COUNT = 32;
    private static final int QUEUE_CAPACITY = 10000;

    private static final AtomicInteger successfulRequests = new AtomicInteger(0);
    private static final AtomicInteger failedRequests = new AtomicInteger(0);
    private static final List<RequestRecord> records = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        BlockingQueue<JsonObject> eventQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        LiftRideEventProducer producer = new LiftRideEventProducer(eventQueue);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        ExecutorService phase1Executor = Executors.newFixedThreadPool(INITIAL_THREAD_COUNT);
        SkiersClient client = new SkiersClient(records);

        for (int i = 0; i < INITIAL_THREAD_COUNT; i++) {
            phase1Executor.submit(() -> {
                for (int j = 0; j < REQUESTS_PER_THREAD; j++) {
                    try {
                        JsonObject liftRide = eventQueue.take();
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

        phase1Executor.shutdown();
        while (!phase1Executor.isTerminated()) {
            Thread.yield();
        }

        int remainingRequests = TOTAL_REQUESTS - (INITIAL_THREAD_COUNT * REQUESTS_PER_THREAD);
        if (remainingRequests > 0) {
            ExecutorService phase2Executor = Executors.newFixedThreadPool(PHASE2_THREAD_COUNT);
            int requestsPerThread = remainingRequests / PHASE2_THREAD_COUNT;
            int extraRequests = remainingRequests % PHASE2_THREAD_COUNT;

            for (int i = 0; i < PHASE2_THREAD_COUNT; i++) {
                final int threadRequests = i == 0 ? requestsPerThread + extraRequests : requestsPerThread;
                if (threadRequests > 0) {
                    phase2Executor.submit(() -> {
                        for (int j = 0; j < threadRequests; j++) {
                            try {
                                JsonObject liftRide = eventQueue.take();
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

            phase2Executor.shutdown();
            while (!phase2Executor.isTerminated()) {
                Thread.yield();
            }
        }

        producer.stop();
        try {
            producerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double throughput = (double) successfulRequests.get() / (totalTime / 1000.0);

        System.out.println("Client Configuration:");
        System.out.println("Initial Thread Count: " + INITIAL_THREAD_COUNT);
        System.out.println("Requests per Thread: " + REQUESTS_PER_THREAD);
        System.out.println("Phase 2 Thread Count: " + PHASE2_THREAD_COUNT);
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
        List<Long> latencies = records.stream()
                .map(RequestRecord::getLatency)
                .sorted()
                .collect(Collectors.toList());

        double meanResponseTime = latencies.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0);

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