package com.cs6650.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SkierLiftRideConsumer {
    private static final Logger logger = Logger.getLogger(SkierLiftRideConsumer.class.getName());
    private static final String QUEUE_NAME = "skier_lift_rides";
    private static final Gson gson = new Gson();

    private static final Map<Integer, SkierRecord> skierRecords = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        String rabbitHost = System.getenv("RABBITMQ_HOST");
        if (rabbitHost == null || rabbitHost.isEmpty()) {
            rabbitHost = "localhost";
        }

        String rabbitUsername = System.getenv("RABBITMQ_USERNAME");
        if (rabbitUsername == null || rabbitUsername.isEmpty()) {
            rabbitUsername = "guest";
        }

        String rabbitPassword = System.getenv("RABBITMQ_PASSWORD");
        if (rabbitPassword == null || rabbitPassword.isEmpty()) {
            rabbitPassword = "guest";
        }

        int numThreads = 80;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setPort(5672);
        factory.setUsername(rabbitUsername);
        factory.setPassword(rabbitPassword);

        logger.info("Connecting to RabbitMQ at " + rabbitHost);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        try {
            Connection connection = factory.newConnection();

            for (int i = 0; i < numThreads; i++) {
                executorService.submit(() -> {
                    try {
                        Channel channel = connection.createChannel();
                        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

                        channel.basicQos(20);

                        logger.info("Consumer thread started, waiting for messages...");

                        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                            processMessage(message);
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        };

                        // Start consuming (false = manual acknowledgment)
                        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, "Error in consumer thread", e);
                    }
                });
            }

            executorService.submit(() -> {
                while (true) {
                    try {
                        Thread.sleep(10000); // Report every 10 seconds
                        logger.info("Current skier records count: " + skierRecords.size());
                        logger.info("Total lift rides processed: " +
                                skierRecords.values().stream()
                                        .mapToInt(SkierRecord::getTotalLiftRides)
                                        .sum());
                    } catch (InterruptedException e) {
                        logger.log(Level.WARNING, "Monitoring thread interrupted", e);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });

            logger.info("Consumer application started. Press Ctrl+C to exit.");
            Thread.currentThread().join();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to start consumer", e);
        } finally {
            executorService.shutdown();
        }
    }

    private static void processMessage(String message) {
        try {
            JsonObject liftRide = gson.fromJson(message, JsonObject.class);
            int skierID = liftRide.get("skierID").getAsInt();
            int liftID = liftRide.get("liftID").getAsInt();
            int verticalGain = liftID * 10; // Simple calculation for vertical gain

            skierRecords.compute(skierID, (key, record) -> {
                if (record == null) {
                    record = new SkierRecord(skierID);
                }
                record.addLiftRide(liftID, verticalGain);
                return record;
            });

        } catch (Exception e) {
            logger.log(Level.WARNING, "Error processing message: " + message, e);
        }
    }
}