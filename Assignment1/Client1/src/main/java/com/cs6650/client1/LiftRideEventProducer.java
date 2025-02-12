package com.cs6650.client1;

import com.google.gson.JsonObject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class LiftRideEventProducer implements Runnable {
    private final BlockingQueue<JsonObject> eventQueue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private static final int BATCH_SIZE = 1000;

    public LiftRideEventProducer(BlockingQueue<JsonObject> queue) {
        this.eventQueue = queue;
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                for (int i = 0; i < BATCH_SIZE && running.get(); i++) {
                    JsonObject event = LiftRideGenerator.generateRandomLiftRide();
                    if (!eventQueue.offer(event)) {
                        Thread.sleep(1);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void stop() {
        running.set(false);
    }
}