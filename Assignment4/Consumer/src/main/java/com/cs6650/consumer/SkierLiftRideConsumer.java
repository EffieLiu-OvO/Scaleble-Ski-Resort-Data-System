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
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SkierLiftRideConsumer is responsible for:
 * <ul>
 *   <li>Consuming skier lift ride messages from RabbitMQ</li>
 *   <li>Batch writing lift ride records to DynamoDB</li>
 *   <li>Maintaining in-memory skier day summaries and resort day summaries</li>
 *   <li>Periodically flushing summaries into DynamoDB</li>
 * </ul>
 */
public class SkierLiftRideConsumer {
    private static final Logger logger = Logger.getLogger(SkierLiftRideConsumer.class.getName());
    private static final String QUEUE_NAME = "skier_lift_rides";
    private static final Gson gson = new Gson();
    // Nested map to store skier records: skierID -> resortID -> dayID -> SkierRecord
    private static final Map<Integer, Map<Integer, Map<Integer, SkierRecord>>> skierResortDayRecords = new ConcurrentHashMap<>();
    // Summary records for resort-day data: resortID#seasonID#dayID -> ResortDaySummary
    private static final Map<String, ResortDaySummary> resortDaySummaries = new ConcurrentHashMap<>();
    private static final int BATCH_THRESHOLD = 20;
    private static final ThreadLocal<List<SkierLiftRide>> threadLocalLiftRideBatch = ThreadLocal.withInitial(ArrayList::new);
    // Executor service for background batch writes
    private static final ExecutorService batchWriteExecutor = Executors.newFixedThreadPool(40);
    private static final AtomicLong messageCounter = new AtomicLong(0);
    private static final AtomicLong lastCount = new AtomicLong(0);
    private static long lastTimestamp = System.currentTimeMillis();
    private static DynamoDBManager dbManager;
    private static final Map<String, SkierVerticalSummary> skierVerticalSummaries = new ConcurrentHashMap<>();
    // In-memory tracking of unique skier IDs and visit counts per resort-day
    private static final Map<String, Set<Integer>> resortDayNewSkiers = new ConcurrentHashMap<>();
    private static final Map<String, Integer> resortDayTotalVisits = new ConcurrentHashMap<>();


    public static void main(String[] args) {
        // whether to use local DynamoDB
        // boolean useLocalDynamoDB = false;
        boolean useLocalDynamoDB = true;
        try{
            // Initialize database manager
            dbManager = new DynamoDBManager(useLocalDynamoDB);
            dbManager.createTablesIfNotExist();

            try {
                SkierDaySummary testSummary = new SkierDaySummary();
                testSummary.setId("test");
                testSummary.setSkierID(999);
                testSummary.setDayID(1);
                testSummary.setResortID(10);
                testSummary.setSeasonID(2);
                testSummary.setTotalRides(1);
                testSummary.setTotalVertical(10);
                HashSet<Integer> testLifts = new HashSet<>();
                testLifts.add(1);
                testSummary.setLiftsRidden(testLifts);

                List<SkierDaySummary> testList = new ArrayList<>();
                testList.add(testSummary);
                dbManager.batchUpdateSkierDaySummaries(testList);

                logger.info("Successfully wrote test record to SkierDaySummaries");


                // testing multiple seasons
                List<SkierVerticalSummary> testSummaries = new ArrayList<>();

                // Season 2023
                SkierVerticalSummary season2023 = new SkierVerticalSummary();
                season2023.setId("999#2023");
                season2023.setSkierID(999);
                season2023.setSeasonID(2023);
                season2023.setTotalVertical(50);

                // Season 2024
                SkierVerticalSummary season2024 = new SkierVerticalSummary();
                season2024.setId("999#2024");
                season2024.setSkierID(999);
                season2024.setSeasonID(2024);
                season2024.setTotalVertical(70);

                testSummaries.add(season2023);
                testSummaries.add(season2024);

                dbManager.batchUpdateSkierVerticalSummary(testSummaries);
                logger.info("Successfully wrote test records for multiple seasons");

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

            // Start multiple consumer threads
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
            // Monitoring thread: logs message throughput every 5 seconds
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
//                        logger.info(String.format("Processing speed: %.2f messages/second", messagesPerSecond));
//                        logger.info("Total messages processed: " + current);
//                        logger.info("Current skier records count: " + skierResortDayRecords.size());
                    } catch(InterruptedException e){
                        logger.log(Level.WARNING,"Monitoring thread interrupted", e);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });

            // Periodically flush aggregated summaries to DB
            executorService.submit(() -> {
                try {
//                    Thread.sleep(30000);
                    Thread.sleep(1000);
                    while(true) {
                        try {
                            processSummaryData();
                        } catch(Exception e) {
                            logger.log(Level.SEVERE, "Error in summary processing", e);
                        }
//                        Thread.sleep(30000);
                        // Interval between flushes
                        Thread.sleep(1000);
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

    /**
     * Periodically processes in-memory skier records and writes summarized data to DynamoDB.
     *
     * This method generates:
     * - SkierDaySummary records (per skier, resort, season, day)
     * - ResortDaySummary records (per resort, season, day)
     *
     * Summaries are batched and written to DynamoDB in bulk.
     */
    private static void processSummaryData() {
        try {
            if (skierResortDayRecords.isEmpty()) {
                logger.info("No skier records to process for summaries");
                return;
            }

            List<SkierDaySummary> skierSummaries = new ArrayList<>();
            List<SkierVerticalSummary> verticalSummaries = new ArrayList<>();

            for (Map.Entry<Integer, Map<Integer, Map<Integer, SkierRecord>>> skierEntry : skierResortDayRecords.entrySet()) {
                int skierID = skierEntry.getKey();
                Map<Integer, Map<Integer, SkierRecord>> resortRecords = skierEntry.getValue();
                Map<Integer, Integer> verticalBySeason = new HashMap<>();

                for (Map.Entry<Integer, Map<Integer, SkierRecord>> resortEntry : resortRecords.entrySet()) {
                    int resortID = resortEntry.getKey();
                    Map<Integer, SkierRecord> dayRecords = resortEntry.getValue();

                    for (Map.Entry<Integer, SkierRecord> dayEntry : dayRecords.entrySet()) {
                        int dayID = dayEntry.getKey();
                        SkierRecord record = dayEntry.getValue();
                        int seasonID = record.getSeasonID();

                        try {

                            // 1. Create SkierDaySummary with proper composite key
                            SkierDaySummary skierSummary = new SkierDaySummary();
                            skierSummary.setIdFromParts(skierID, resortID, seasonID, dayID);
                            skierSummary.setSkierID(skierID);
                            skierSummary.setDayID(dayID);
                            skierSummary.setSeasonID(seasonID);
                            skierSummary.setResortID(resortID);
                            skierSummary.setTotalRides(record.getTotalLiftRides());
                            skierSummary.setTotalVertical(record.getTotalVertical());

                            HashSet<Integer> liftsRidden = new HashSet<>();
                            liftsRidden.add(1);
                            skierSummary.setLiftsRidden(liftsRidden);
                            skierSummaries.add(skierSummary);


                            // 2. Build ResortDaySummary
//                            logger.info(seasonID + "currently is");
                            String resortDayId = resortID + "#"+ seasonID + "#" + dayID;

                            resortDayTotalVisits.merge(resortDayId, record.getTotalLiftRides(), Integer::sum);

                            verticalBySeason.put(seasonID,
                                verticalBySeason.getOrDefault(seasonID, 0) + record.getTotalVertical());

                        } catch (Exception e) {
                            logger.log(Level.WARNING, "Error processing summary for skier " + skierID + " on day " + dayID, e);
                        }
                    }
                }

                for (Map.Entry<Integer, Integer> entry : verticalBySeason.entrySet()) {
                    int seasonID = entry.getKey();
                    int vertical = entry.getValue();

                    SkierVerticalSummary verticalSummary = new SkierVerticalSummary();
                    verticalSummary.setSkierID(skierID);
                    verticalSummary.setSeasonID(seasonID);
                    verticalSummary.setId(skierID + "#" + seasonID);
                    verticalSummary.setTotalVertical(vertical);

                    verticalSummaries.add(verticalSummary);
                }
                if (verticalBySeason.size() > 1) {
                    int totalVertical = verticalBySeason.values().stream().mapToInt(Integer::intValue).sum();
                    SkierVerticalSummary allSummary = new SkierVerticalSummary();
                    allSummary.setSkierID(skierID);
                    allSummary.setSeasonID(-1);
                    allSummary.setId(skierID + "#ALL");
                    allSummary.setTotalVertical(totalVertical);
                    verticalSummaries.add(allSummary);
                }
            }

            // Save skier summaries to DB
            if (!skierSummaries.isEmpty()) {
                try {
                    dbManager.batchUpdateSkierDaySummaries(skierSummaries);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to update skier day summaries", e);
                    for (SkierDaySummary summary : skierSummaries) {
                        try {
                            dbManager.batchUpdateSkierDaySummaries(List.of(summary));
                        } catch (Exception ex) {
                            logger.log(Level.SEVERE, "Failed to update skier summary: " + summary.getId(), ex);
                        }
                    }
                }
            }
            if (!verticalSummaries.isEmpty()) {
                dbManager.batchUpdateSkierVerticalSummary(verticalSummaries);
                logger.info("Updated " + verticalSummaries.size() + " skier vertical summaries");
            }

            // Write resort-level summaries
            for (Map.Entry<String, Set<Integer>> entry : resortDayNewSkiers.entrySet()) {
                String resortDayId = entry.getKey();
                Set<Integer> skierSet = entry.getValue();
                int totalRides = resortDayTotalVisits.getOrDefault(resortDayId, 0);

                if (!skierSet.isEmpty()) {
                    String[] parts = resortDayId.split("#");
                    int resortID = Integer.parseInt(parts[0]);
                    int seasonID = Integer.parseInt(parts[1]);
                    int dayID = Integer.parseInt(parts[2]);
                    int increment = skierSet.size();

                    logger.info("Processing resortDayId: " + resortDayId + " with " + increment + " new skiers and "+ totalRides + " rides");;
                    try {
                        logger.info("Calling incrementUniqueSkierCount with: id=" + resortDayId +
                                ", resortID=" + resortID + ", seasonID=" + seasonID +
                                ", dayID=" + dayID + ", increment=" + increment);
                        dbManager.incrementUniqueSkierCount(resortDayId, increment,totalRides, resortID, seasonID, dayID);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Failed to increment unique skier count for " + resortDayId, e);
                    }
                }
            }
            logger.info("Clearing skierResortDayRecords after successful flush.");
            // Clear in-memory state
            skierResortDayRecords.clear();
            resortDayNewSkiers.clear();
            resortDayTotalVisits.clear();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing summary data", e);
        }
    }

    /**
     * Processes an incoming lift ride message.
     *
     * Extracts skier, lift, resort, and day information from the message,
     * updates in-memory summary structures, and batches writes to DynamoDB.
     *
     * @param message JSON string containing lift ride information.
     * @param threadId ID of the consumer thread processing the message.
     */
    private static void processMessage(String message, int threadId) {
        try {
            JsonObject liftRideJson = gson.fromJson(message, JsonObject.class);
            int skierID = liftRideJson.get("skierID").getAsInt();
            int liftID = liftRideJson.get("liftID").getAsInt();
            int resortID = liftRideJson.get("resortID").getAsInt();
            int dayID = liftRideJson.get("dayID").getAsInt();
            int time = liftRideJson.get("time").getAsInt();
            int seasonID = liftRideJson.get("seasonID").getAsInt();
            int verticalGain = liftID * 10;

            String resortDayId = resortID + "#" + seasonID + "#" + dayID;

            // Update in-memory skier record structure
            skierResortDayRecords.computeIfAbsent(skierID, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(resortID, k -> new ConcurrentHashMap<>())
                    .compute(dayID, (key, record) -> {
                        if (record == null) {
                            record = new SkierRecord(skierID, seasonID);
                        }
                        record.addLiftRide(liftID, verticalGain);
                        return record;
                    });


            // Track resort-day unique skier and visit
            resortDayNewSkiers.computeIfAbsent(resortDayId, k -> ConcurrentHashMap.newKeySet()).add(skierID);
            resortDayTotalVisits.merge(resortDayId, 1, Integer::sum);

            // Update in-memory SkierVerticalSummary
            String verticalKey = skierID + "#" + seasonID;
            SkierVerticalSummary summary = skierVerticalSummaries.computeIfAbsent(verticalKey, k -> {
                SkierVerticalSummary s = new SkierVerticalSummary();
                s.setId(verticalKey);
                s.setSkierID(skierID);
                s.setSeasonID(seasonID);
                s.setTotalVertical(0);
                return s;
            });
            summary.setTotalVertical(summary.getTotalVertical() + verticalGain);


            // Build lift ride record
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

            // If threshold reached, submit for async batch save
            if (localBatch.size() >= BATCH_THRESHOLD) {
                List<SkierLiftRide> batchToWrite = new ArrayList<>(localBatch);
                localBatch.clear();
                batchWriteExecutor.submit(() -> {
                    try {
                        dbManager.batchSaveLiftRides(batchToWrite);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Error processing asynchronous batch", e);
                    }
                });
            }
            messageCounter.incrementAndGet();

        } catch (Exception e) {
            logger.log(Level.WARNING, "Error processing message: " + message, e);
        }

    }
}