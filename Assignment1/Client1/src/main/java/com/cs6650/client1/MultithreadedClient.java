package com.cs6650.client1;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import com.google.gson.JsonObject;

public class MultithreadedClient {
    private static final int INITIAL_THREAD_COUNT = 32;
    private static final int REQUESTS_PER_THREAD = 1000;
    private static final int TOTAL_REQUESTS = 200000;
    private static final int PHASE2_THREAD_COUNT = 32;
    private static final int QUEUE_CAPACITY = 10000;

    private static final AtomicInteger successfulRequests = new AtomicInteger(0);
    private static final AtomicInteger failedRequests = new AtomicInteger(0);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        BlockingQueue<JsonObject> eventQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        LiftRideEventProducer producer = new LiftRideEventProducer(eventQueue);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        ExecutorService phase1Executor = Executors.newFixedThreadPool(INITIAL_THREAD_COUNT);
        for (int i = 0; i < INITIAL_THREAD_COUNT; i++) {
            phase1Executor.submit(() -> {
                for (int j = 0; j < REQUESTS_PER_THREAD; j++) {
                    try {
                        JsonObject liftRide = eventQueue.take();
                        if (SkiersClient.sendPostRequest(liftRide)) {
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
                                if (SkiersClient.sendPostRequest(liftRide)) {
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
    }
}