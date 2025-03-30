package com.cs6650.consumer;
import com.cs6650.consumer.db.DynamoDBManager;
import com.cs6650.consumer.model.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SkierLiftRideConsumer {
    private static final Logger logger = Logger.getLogger(SkierLiftRideConsumer.class.getName());
    private static final String QUEUE_NAME = "skier_lift_rides";
    private static final Gson gson = new Gson();
    private static final Map<Integer, Map<Integer, Map<Integer, SkierRecord>>> skierResortDayRecords = new ConcurrentHashMap<>();
    private static final Map<String, ResortDaySummary> resortDaySummaries = new ConcurrentHashMap<>();
    private static final int BATCH_THRESHOLD = 20;
    private static final ThreadLocal<List<SkierLiftRide>> threadLocalLiftRideBatch = ThreadLocal.withInitial(ArrayList::new);
    private static final ExecutorService batchWriteExecutor = Executors.newFixedThreadPool(40);
    private static final AtomicLong messageCounter = new AtomicLong(0);
    private static final AtomicLong lastCount = new AtomicLong(0);
    private static long lastTimestamp = System.currentTimeMillis();
    private static DynamoDBManager dbManager;

    public static void main(String[] args) {
        boolean useLocalDynamoDB = false;
        try{
            dbManager = new DynamoDBManager(useLocalDynamoDB);
            dbManager.createTablesIfNotExist();

            try {
                SkierDaySummary testSummary = new SkierDaySummary();
                testSummary.setId("test");
                testSummary.setSkierID(999);
                testSummary.setDayID(1);
                testSummary.setResortID(10);
                testSummary.setTotalRides(1);
                testSummary.setTotalVertical(10);
                HashSet<Integer> testLifts = new HashSet<>();
                testLifts.add(1);
                testSummary.setLiftsRidden(testLifts);

                List<SkierDaySummary> testList = new ArrayList<>();
                testList.add(testSummary);
                dbManager.batchUpdateSkierDaySummaries(testList);

                logger.info("Successfully wrote test record to SkierDaySummaries");
            } catch(Exception e) {
                logger.log(Level.SEVERE, "Failed to write test record", e);
            }

            String rabbitHost = System.getenv("RABBITMQ_HOST");
            if(rabbitHost==null || rabbitHost.isEmpty()){
                rabbitHost = "localhost";
            }
            String rabbitUsername = System.getenv("RABBITMQ_USERNAME");
            if(rabbitUsername==null || rabbitUsername.isEmpty()){
                rabbitUsername = "guest";
            }
            String rabbitPassword = System.getenv("RABBITMQ_PASSWORD");
            if(rabbitPassword==null || rabbitPassword.isEmpty()){
                rabbitPassword = "guest";
            }
            int numThreads = 300;
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(rabbitHost);
            factory.setPort(5672);
            factory.setUsername(rabbitUsername);
            factory.setPassword(rabbitPassword);
            factory.setConnectionTimeout(30000);
            factory.setRequestedHeartbeat(30);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setNetworkRecoveryInterval(5000);
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            Connection connection = factory.newConnection();
            for(int i=0;i<numThreads;i++){
                final int threadId = i;
                executorService.submit(() -> {
                    try{
                        Channel channel = connection.createChannel();
                        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                        channel.basicQos(50);
                        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                            processMessage(message, threadId);
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        };
                        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
                    } catch(IOException e){
                        logger.log(Level.SEVERE,"Error in consumer thread " + threadId, e);
                    }
                });
            }
            executorService.submit(() -> {
                while(true){
                    try{
                        Thread.sleep(5000);
                        long current = messageCounter.get();
                        long currentTime = System.currentTimeMillis();
                        long count = current - lastCount.getAndSet(current);
                        long timeElapsed = currentTime - lastTimestamp;
                        lastTimestamp = currentTime;
                        double messagesPerSecond = count * 1000.0 / timeElapsed;
                        logger.info(String.format("Processing speed: %.2f messages/second", messagesPerSecond));
                        logger.info("Total messages processed: " + current);
                        logger.info("Current skier records count: " + skierResortDayRecords.size());
                    } catch(InterruptedException e){
                        logger.log(Level.WARNING,"Monitoring thread interrupted", e);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });

            executorService.submit(() -> {
                try {
                    Thread.sleep(30000);
                    while(true) {
                        try {
                            processSummaryData();
                        } catch(Exception e) {
                            logger.log(Level.SEVERE, "Error in summary processing", e);
                        }
                        Thread.sleep(30000);
                    }
                } catch(InterruptedException e) {
                    logger.log(Level.WARNING, "Summary thread interrupted", e);
                    Thread.currentThread().interrupt();
                }
            });

            Thread.currentThread().join();
        } catch(Exception e){
            logger.log(Level.SEVERE,"Failed to start consumer", e);
        }
    }

    private static void processSummaryData() {
        try {
            if (skierResortDayRecords.isEmpty()) {
                logger.info("No skier records to process for summaries");
                return;
            }

            List<SkierDaySummary> skierSummaries = new ArrayList<>();
            Map<String, ResortDaySummary> batchResortSummaries = new HashMap<>();

            for (Map.Entry<Integer, Map<Integer, Map<Integer, SkierRecord>>> skierEntry : skierResortDayRecords.entrySet()) {
                int skierID = skierEntry.getKey();
                Map<Integer, Map<Integer, SkierRecord>> resortRecords = skierEntry.getValue();

                for (Map.Entry<Integer, Map<Integer, SkierRecord>> resortEntry : resortRecords.entrySet()) {
                    int resortID = resortEntry.getKey();
                    Map<Integer, SkierRecord> dayRecords = resortEntry.getValue();

                    for (Map.Entry<Integer, SkierRecord> dayEntry : dayRecords.entrySet()) {
                        int dayID = dayEntry.getKey();
                        SkierRecord record = dayEntry.getValue();

                        try {
                            String skierSummaryId = skierID + "#" + dayID;
                            SkierDaySummary skierSummary = new SkierDaySummary();
                            skierSummary.setId(skierSummaryId);
                            skierSummary.setSkierID(skierID);
                            skierSummary.setDayID(dayID);
                            skierSummary.setResortID(resortID);
                            skierSummary.setTotalRides(record.getTotalLiftRides());
                            skierSummary.setTotalVertical(record.getTotalVertical());

                            HashSet<Integer> liftsRidden = new HashSet<>();
                            liftsRidden.add(1);
                            skierSummary.setLiftsRidden(liftsRidden);

                            skierSummaries.add(skierSummary);

                            String resortDayId = resortID + "#" + dayID;
                            ResortDaySummary resortSummary = batchResortSummaries.get(resortDayId);
                            if (resortSummary == null) {
                                resortSummary = resortDaySummaries.getOrDefault(resortDayId, new ResortDaySummary());
                                resortSummary.setId(resortDayId);
                                resortSummary.setResortID(resortID);
                                resortSummary.setDayID(dayID);

                                HashSet<Integer> uniqueSkiers = new HashSet<>();
                                uniqueSkiers.add(skierID);
                                resortSummary.setUniqueSkiers(uniqueSkiers);

                                batchResortSummaries.put(resortDayId, resortSummary);
                            } else {
                                resortSummary.getUniqueSkiers().add(skierID);
                            }
                        } catch (Exception e) {
                            logger.log(Level.WARNING, "Error processing summary for skier " + skierID + " on day " + dayID, e);
                        }
                    }
                }
            }

            if (!skierSummaries.isEmpty()) {
                try {
                    dbManager.batchUpdateSkierDaySummaries(skierSummaries);
                    logger.info("Updated " + skierSummaries.size() + " skier day summaries");
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to update skier day summaries", e);
                    for (SkierDaySummary summary : skierSummaries) {
                        try {
                            List<SkierDaySummary> singleItem = new ArrayList<>();
                            singleItem.add(summary);
                            dbManager.batchUpdateSkierDaySummaries(singleItem);
                        } catch (Exception ex) {
                            logger.log(Level.SEVERE, "Failed to update skier summary: " + summary.getId(), ex);
                        }
                    }
                }
            }

            if (!batchResortSummaries.isEmpty()) {
                try {
                    List<ResortDaySummary> resortList = new ArrayList<>(batchResortSummaries.values());
                    dbManager.batchUpdateResortDaySummaries(resortList);
                    logger.info("Updated " + resortList.size() + " resort day summaries");

                    for (ResortDaySummary summary : resortList) {
                        resortDaySummaries.put(summary.getId(), summary);
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to update resort day summaries", e);
                    for (ResortDaySummary summary : batchResortSummaries.values()) {
                        try {
                            List<ResortDaySummary> singleItem = new ArrayList<>();
                            singleItem.add(summary);
                            dbManager.batchUpdateResortDaySummaries(singleItem);
                        } catch (Exception ex) {
                            logger.log(Level.SEVERE, "Failed to update resort summary: " + summary.getId(), ex);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing summary data", e);
        }
    }

    private static void processMessage(String message, int threadId){
        try{
            JsonObject liftRideJson = gson.fromJson(message, JsonObject.class);
            int skierID = liftRideJson.get("skierID").getAsInt();
            int liftID = liftRideJson.get("liftID").getAsInt();
            int resortID = liftRideJson.get("resortID").getAsInt();
            int dayID = liftRideJson.get("dayID").getAsInt();
            int time = liftRideJson.get("time").getAsInt();
            int verticalGain = liftID * 10;

            skierResortDayRecords.computeIfAbsent(skierID, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(resortID, k -> new ConcurrentHashMap<>())
                    .compute(dayID, (key, record) -> {
                        if(record == null) {
                            record = new SkierRecord(skierID);
                        }
                        record.addLiftRide(liftID, verticalGain);
                        return record;
                    });

            SkierLiftRide liftRide = new SkierLiftRide();
            liftRide.setId(skierID + "#" + dayID + "#" + time);
            liftRide.setSkierID(skierID);
            liftRide.setResortID(resortID);
            liftRide.setDayID(dayID);
            liftRide.setLiftID(liftID);
            liftRide.setTime(time);
            liftRide.setVertical(verticalGain);
            List<SkierLiftRide> localBatch = threadLocalLiftRideBatch.get();
            localBatch.add(liftRide);
            if(localBatch.size()>=BATCH_THRESHOLD){
                List<SkierLiftRide> batchToWrite = new ArrayList<>(localBatch);
                localBatch.clear();
                batchWriteExecutor.submit(() -> {
                    try{
                        dbManager.batchSaveLiftRides(batchToWrite);
                        logger.info("Asynchronously processed batch of " + batchToWrite.size() + " items.");
                    } catch(Exception e){
                        logger.log(Level.SEVERE,"Error processing asynchronous batch", e);
                    }
                });
            }
            messageCounter.incrementAndGet();
        } catch(Exception e){
            logger.log(Level.WARNING,"Error processing message: " + message, e);
        }
    }
}