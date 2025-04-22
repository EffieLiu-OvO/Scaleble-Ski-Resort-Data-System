package com.cs6650.consumer.db;
import com.cs6650.consumer.model.*;
import java.util.HashSet;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.services.dynamodb.model.*;
public class DynamoDBManager {
    private static final Logger logger = Logger.getLogger(DynamoDBManager.class.getName());
    private final DynamoDbClient dynamoDbClient;
    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<SkierLiftRide> liftRideTable;
    private final DynamoDbTable<SkierDaySummary> skierDaySummaryTable;
    private final DynamoDbTable<ResortDaySummary> resortDaySummaryTable;
    private final DynamoDbTable<SkierVerticalSummary> skierVerticalTable;

    // Constructor to initialize DynamoDB client and tables.
    public DynamoDBManager(boolean useLocal) {
        if(useLocal){
            RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(5).build();
            ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofSeconds(30)).retryPolicy(retryPolicy).build();
            this.dynamoDbClient = DynamoDbClient.builder().endpointOverride(URI.create("http://localhost:8000")).region(Region.US_WEST_2).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy","dummy"))).overrideConfiguration(clientConfig).build();
        } else {
            this.dynamoDbClient = DynamoDbClient.builder().region(Region.US_WEST_2).build();
        }
        this.enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();
        this.liftRideTable = enhancedClient.table("SkierLiftRides",TableSchema.fromBean(SkierLiftRide.class));
        this.skierDaySummaryTable = enhancedClient.table("SkierDaySummaries",TableSchema.fromBean(SkierDaySummary.class));
        this.resortDaySummaryTable = enhancedClient.table("ResortDaySummaries",TableSchema.fromBean(ResortDaySummary.class));
        this.skierVerticalTable = enhancedClient.table("SkierVerticalSummaries",TableSchema.fromBean(SkierVerticalSummary.class));
        testConnection();
    }
    public void testConnection(){
        try{
            ListTablesResponse response = dynamoDbClient.listTables();
            logger.info("Successfully connected to DynamoDB. Found tables: " + response.tableNames());
        } catch(Exception e){
            logger.log(Level.SEVERE,"Failed to connect to DynamoDB",e);
            throw new RuntimeException("Failed to connect to DynamoDB",e);
        }
    }
    // Create tables
//    public void createTablesIfNotExist(){
//        try{
//            liftRideTable.createTable(builder->builder.provisionedThroughput(b->b.readCapacityUnits(100L).writeCapacityUnits(2000L).build()));
//            logger.info("Successfully created SkierLiftRides table");
//            skierDaySummaryTable.createTable(builder->builder.provisionedThroughput(b->b.readCapacityUnits(50L).writeCapacityUnits(200L).build()));
//            logger.info("Successfully created SkierDaySummaries table");
//            resortDaySummaryTable.createTable(builder->builder.provisionedThroughput(b->b.readCapacityUnits(50L).writeCapacityUnits(200L).build()));
//            logger.info("Successfully created ResortDaySummaries table");
//        } catch(ResourceInUseException e){
//            logger.info("Tables already exist, continuing");
//        } catch(Exception e){
//            logger.log(Level.SEVERE,"Error creating tables",e);
//            throw new RuntimeException("Failed to create DynamoDB tables",e);
//        }
//    }

    // wy version

    /**
     * Creates required tables if they do not already exist.
     */
    public void createTablesIfNotExist() {
        try {
            List<String> existingTables = dynamoDbClient.listTables().tableNames();

            if (!existingTables.contains("SkierLiftRides")) {
                createTable("SkierLiftRides");
            }
            if (!existingTables.contains("SkierDaySummaries")) {
                createTable("SkierDaySummaries");
            }
            if (!existingTables.contains("ResortDaySummaries")) {
                createTable("ResortDaySummaries");
            }
            if (!existingTables.contains("SkierVerticalSummaries")) {
                createTable("SkierVerticalSummaries");
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating tables", e);
            throw new RuntimeException("Failed to create DynamoDB tables", e);
        }
    }

    // wy version
    /**
     *  Creates a single table with id as the primary key.
     * @param tableName the name of the table to create.
     */
    private void createTable(String tableName) {
        logger.info("Creating table: " + tableName);
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName("id")
                                .attributeType(ScalarAttributeType.S)
                                .build()
                )
                .keySchema(
                        KeySchemaElement.builder()
                                .attributeName("id")
                                .keyType(KeyType.HASH)
                                .build()
                )
                .provisionedThroughput(
                        ProvisionedThroughput.builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build()
                )
                .build();
        dynamoDbClient.createTable(request);
        waitForTableToBeActive(tableName);
        logger.info("Created and active: " + tableName);
    }

    // wy version
    /**
     * Waits for a table to become active after creation.
     * @param tableName the name of the table.
     */
    private void waitForTableToBeActive(String tableName) {
        while (true) {
            DescribeTableResponse response = dynamoDbClient.describeTable(
                    DescribeTableRequest.builder().tableName(tableName).build()
            );
            if (response.table().tableStatusAsString().equals("ACTIVE")) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for table to become ACTIVE");
            }
        }
    }


    public void saveLiftRide(SkierLiftRide liftRide){
        try{
            liftRideTable.putItem(liftRide);
        } catch(Exception e){
            logger.log(Level.SEVERE,"Failed to save lift ride: " + liftRide.getId(),e);
            throw e;
        }
    }
    public void batchSaveLiftRides(List<SkierLiftRide> rides){
        if(rides.isEmpty()) return;
        try{
            final int maxBatchSize = 25;
            List<List<SkierLiftRide>> batches = new ArrayList<>();
            for(int i=0;i<rides.size();i+=maxBatchSize){
                batches.add(rides.subList(i, Math.min(i+maxBatchSize, rides.size())));
            }
            long startTime = System.currentTimeMillis();
            for(List<SkierLiftRide> batch : batches){
                WriteBatch.Builder<SkierLiftRide> builder = WriteBatch.builder(SkierLiftRide.class).mappedTableResource(liftRideTable);
                for(SkierLiftRide ride : batch){
                    builder.addPutItem(ride);
                }
                try{
                    enhancedClient.batchWriteItem(BatchWriteItemEnhancedRequest.builder().writeBatches(builder.build()).build());
                } catch(Exception e){
                    logger.log(Level.SEVERE,"Error during batch write, retrying with individual writes",e);
                    for(SkierLiftRide ride : batch){
                        try{
                            liftRideTable.putItem(ride);
                        } catch(Exception e2){
                            logger.log(Level.SEVERE,"Failed to write individual item: " + ride.getId(),e2);
                        }
                    }
                }
            }
            long duration = System.currentTimeMillis()-startTime;
//            logger.info(String.format("Batch save of %d items completed in %d ms (%.2f items/second)", rides.size(), duration, (rides.size()*1000.0)/duration));
        } catch(Exception e){
            logger.log(Level.SEVERE,"Failed to batch save lift rides",e);
            throw e;
        }
    }
    
    // tw
    /**
     * Batch updates skier day summaries, merging records with matching IDs.
     * For duplicate IDs: sums totals, merges lifts, and auto-generates missing IDs.
     * @param summaries list of SkierDaySummary records to update/merge
     */
    public void batchUpdateSkierDaySummaries(List<SkierDaySummary> summaries) {
        if (summaries.isEmpty()) {
            logger.info("No summaries to update");
            return;
        }
    
        try {
            Map<String, SkierDaySummary> mergedSummaries = new HashMap<>();
        
            for (SkierDaySummary summary : summaries) {
                // Ensure ID is properly set
                if (summary.getId() == null || summary.getId().isEmpty()) {
                    summary.setIdFromParts(summary.getSkierID(), summary.getResortID(),
                            summary.getSeasonID(), summary.getDayID());
                }
                
                String id = summary.getId();
                SkierDaySummary existing = mergedSummaries.get(id);
            
                if (existing != null) {
                    // Merge with existing summary
                    existing.setTotalRides(existing.getTotalRides() + summary.getTotalRides());
                    existing.setTotalVertical(existing.getTotalVertical() + summary.getTotalVertical());
                
                    // Ensure liftsRidden is not null before merging
                    if (summary.getLiftsRidden() != null) {
                        existing.getLiftsRidden().addAll(summary.getLiftsRidden());
                    }
                } else {
                    // Add new summary to map (defensive copy optional)
                    mergedSummaries.put(id, summary);
                }
            }
        
            // Batch write to DynamoDB
            for (SkierDaySummary summary : mergedSummaries.values()) {
                skierDaySummaryTable.putItem(summary);
            }
        
            logger.info("Successfully updated " + mergedSummaries.size() +
                    " skier day summaries (after merging)");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to batch update skier day summaries", e);
            throw e;
        }
    }

    // wy version
    /**
     * Batch updates a list of ResortDaySummary records.
     * If multiple records have the same ID, their unique skiers are merged.
     * @param summaries list of ResortDaySummary records.
     */
    public void batchUpdateResortDaySummaries(List<ResortDaySummary> summaries) {
        if (summaries.isEmpty()) {
            logger.warning("No resort day summaries provided for batch update.");
            return;
        }

        try {
            // Use a map to merge summaries by ID
            Map<String, ResortDaySummary> mergedSummaries = new HashMap<>();

            for (ResortDaySummary summary : summaries) {
                // // Auto-generate ID if missing
                if (summary.getId() == null || summary.getId().isEmpty()) {
                    summary.setIdFromParts(summary.getResortID(), summary.getSeasonID(), summary.getDayID());
                }
                String id = summary.getId();

                // If already exists, merge the count
                ResortDaySummary existing = mergedSummaries.get(id);
                if (existing != null) {
                    existing.setUniqueSkierCount(
                            existing.getUniqueSkierCount() + summary.getUniqueSkierCount()
                    );
                } else {
                    mergedSummaries.put(id, summary);
                }
            }

            // Write merged summaries to DynamoDB
            for (ResortDaySummary summary : mergedSummaries.values()) {
                logger.info("Writing to DynamoDB: id=" + summary.getId() +
                        ", resortID=" + summary.getResortID() +
                        ", seasonID=" + summary.getSeasonID() +
                        ", dayID=" + summary.getDayID() +
                        ", uniqueSkierCount=" + summary.getUniqueSkierCount());
                resortDaySummaryTable.putItem(summary);
            }

            logger.info("Successfully updated " + mergedSummaries.size() + " resort day summaries (merged by count)");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to batch update resort day summaries", e);
            throw e;
        }
    }


    public int getSkierDaysCount(int skierID){
        try{
            java.util.Set<Integer> skiDays = new java.util.HashSet<>();
            for(SkierDaySummary summary : skierDaySummaryTable.scan().items()){
                if(summary.getSkierID()==skierID){
                    skiDays.add(summary.getDayID());
                }
            }
            return skiDays.size();
        } catch(Exception e){
            logger.log(Level.SEVERE,"Failed to get skier days count for skierID: " + skierID,e);
            return 0;
        }
    }

    // wy version
    /**
     * Retrieves a ResortDaySummary record by resort, season, and day IDs.
     * @param resortID resort ID.
     * @param seasonID season ID.
     * @param dayID day ID.
     * @return ResortDaySummary record, or null if not found.
     */
    public ResortDaySummary getResortDaySummary(int resortID, int seasonID, int dayID) {
        String resortDayId = resortID + "#" + seasonID + "#" + dayID;
        try {
            ResortDaySummary key = new ResortDaySummary();
            key.setId(resortDayId); // // Must set ID for primary key lookup
            return resortDaySummaryTable.getItem(key);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to fetch ResortDaySummary for id: " + resortDayId, e);
            return null;
        }
    }
    
    // tw version
    /**
     * Retrieves a SkierDaySummary record by skier, day, season, and resort IDs.
     * @param skierID skier ID
     * @param dayID day ID
     * @param seasonID season ID
     * @param resortID resort ID
     * @return SkierDaySummary record, or null if not found
     */
    public SkierDaySummary getSkierDaySummary(int skierID, int dayID, int seasonID, int resortID) {
        String compositeId = skierID + "#" + resortID + "#" + seasonID + "#" + dayID;
        
        try {
            SkierDaySummary key = new SkierDaySummary();
            key.setId(compositeId);
            
            // Get item from DynamoDB using the composite key
            return skierDaySummaryTable.getItem(key);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to fetch SkierDaySummary for id: " + compositeId, e);
            return null;
        }
    }

    public void batchUpdateSkierVerticalSummary(List<SkierVerticalSummary> summaries) {
        if (summaries.isEmpty()) return;

        try {
            Map<String, SkierVerticalSummary> mergedBySeason = new HashMap<>();
            Map<Integer, Integer> totalBySkier = new HashMap<>();
            Map<Integer, Set<Integer>> skierSeasons = new HashMap<>();

            // Merge per-season entries
            for (SkierVerticalSummary summary : summaries) {
                String id = summary.getId();
                SkierVerticalSummary existing = mergedBySeason.get(id);

                if (existing != null) {
                    existing.setTotalVertical(existing.getTotalVertical() + summary.getTotalVertical());
                } else {
                    mergedBySeason.put(id, summary);
                }
            }

            // Save season-specific entries and track season count per skier
            for (SkierVerticalSummary summary : mergedBySeason.values()) {
                skierVerticalTable.putItem(summary);

                int skierID = summary.getSkierID();
                int seasonID = summary.getSeasonID();

                totalBySkier.put(skierID,
                    totalBySkier.getOrDefault(skierID, 0) + summary.getTotalVertical());

                skierSeasons.computeIfAbsent(skierID, k -> new HashSet<>()).add(seasonID);
            }

            // Save #ALL entries only if skier has >1 unique season
            for (Map.Entry<Integer, Integer> entry : totalBySkier.entrySet()) {
                int skierID = entry.getKey();
                Set<Integer> seasons = skierSeasons.getOrDefault(skierID, Set.of());

                if (seasons.size() > 1) {
                    logger.info("Writing #ALL summary for skierID=" + skierID + " totalVertical=" + entry.getValue());
                    SkierVerticalSummary allSummary = new SkierVerticalSummary();
                    allSummary.setSkierID(skierID);
                    allSummary.setSeasonID(-1);
                    allSummary.setId(skierID + "#ALL");
                    allSummary.setTotalVertical(entry.getValue());
                    skierVerticalTable.putItem(allSummary);
                }
            }

            logger.info("Updated " + mergedBySeason.size() + " per-season skier vertical summaries");
            logger.info("Total batch size (after merge): " + summaries.size());

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to batch update skier vertical summaries", e);
            throw e;
        }
    }


//    public void batchUpdateSkierVerticalSummary(List<SkierVerticalSummary> summaries) {
//        if (summaries.isEmpty()) return;
//
//        try {
//            Map<String, SkierVerticalSummary> mergedBySeason = new HashMap<>();
//            Map<Integer, Integer> totalBySkier = new HashMap<>();
//
//            for (SkierVerticalSummary summary : summaries) {
//                // Merge per-season
//                String id = summary.getId();
//                SkierVerticalSummary existing = mergedBySeason.get(id);
//
//                if (existing != null) {
//                    existing.setTotalVertical(existing.getTotalVertical() + summary.getTotalVertical());
//                } else {
//                    mergedBySeason.put(id, summary);
//                }
//
//                // Track total per skier
//                int skierID = summary.getSkierID();
//                totalBySkier.put(skierID,
//                    totalBySkier.getOrDefault(skierID, 0) + summary.getTotalVertical());
//            }
//
//            // Save season-specific entries
//            for (SkierVerticalSummary summary : mergedBySeason.values()) {
//                skierVerticalTable.putItem(summary);
//            }
//
//            // Save #ALL entries only if skier has >1 season
//            for (Map.Entry<Integer, Integer> entry : totalBySkier.entrySet()) {
//                int skierID = entry.getKey();
//                long seasonCount = summaries.stream()
//                    .filter(s -> s.getSkierID() == skierID)
//                    .map(SkierVerticalSummary::getSeasonID)
//                    .distinct().count();
//
//                if (seasonCount > 1) {
//                    logger.info("Writing ALL summary for skierID=" + skierID + " totalVertical=" + entry.getValue());
//                    SkierVerticalSummary allSummary = new SkierVerticalSummary();
//                    allSummary.setSkierID(skierID);
//                    allSummary.setSeasonID(-1);
//                    allSummary.setId(skierID + "#ALL");
//                    allSummary.setTotalVertical(entry.getValue());
//                    skierVerticalTable.putItem(allSummary);
//                }
//            }
//
//            logger.info("Incoming summary batch size: " + summaries.size());
//            summaries.forEach(s -> logger.info("skier=" + s.getSkierID() + ", season=" + s.getSeasonID()));
//
//
//        } catch (Exception e) {
//            logger.log(Level.SEVERE, "Failed to batch update skier vertical summaries", e);
//            throw e;
//        }
//    }

    public SkierVerticalSummary getSkierVerticalSummary(int skierID, Integer seasonID) {
        try {
            if (seasonID != null) {
                String id = skierID + "#" + seasonID;
                SkierVerticalSummary key = new SkierVerticalSummary();
                key.setId(id);
                key.setSeasonID(seasonID);
                return skierVerticalTable.getItem(key);
            } else {
                // First try #ALL
                String allId = skierID + "#ALL";
                SkierVerticalSummary allKey = new SkierVerticalSummary();
                allKey.setId(allId);
                allKey.setSeasonID(-1);
                SkierVerticalSummary allSummary = skierVerticalTable.getItem(allKey);
                if (allSummary != null) return allSummary;

                // Else scan and sum
                int total = 0;
                for (SkierVerticalSummary summary : skierVerticalTable.scan().items()) {
                    if (summary.getSkierID() == skierID && summary.getSeasonID() != -1) {
                        total += summary.getTotalVertical();
                    }
                }

                if (total == 0) return null;

                SkierVerticalSummary fallback = new SkierVerticalSummary();
                fallback.setSkierID(skierID);
                fallback.setSeasonID(-1);
                fallback.setId(skierID + "#ALL");
                fallback.setTotalVertical(total);
                return fallback;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to fetch skier vertical summary for skierID=" + skierID, e);
            return null;
        }
    }

    public void runDiagnostics(){
        logger.info("Running DynamoDB diagnostics...");
        try{
            ListTablesResponse tables = dynamoDbClient.listTables();
            logger.info("Tables in DynamoDB: " + tables.tableNames());
            try{
                int liftRideCount = 0;
                int sampleLimit = 100;
                for(SkierLiftRide item : liftRideTable.scan().items()){
                    liftRideCount++;
                    if(liftRideCount>=sampleLimit) break;
                }
                logger.info("SkierLiftRides sample count: " + liftRideCount + " (limited to " + sampleLimit + " records)");
            } catch(Exception e){
                logger.log(Level.SEVERE,"Failed to count SkierLiftRides",e);
            }
            try{
                int summaryCount = 0;
                int sampleLimit = 100;
                for(SkierDaySummary item : skierDaySummaryTable.scan().items()){
                    summaryCount++;
                    if(summaryCount>=sampleLimit) break;
                }
                logger.info("SkierDaySummaries sample count: " + summaryCount + " (limited to " + sampleLimit + " records)");
            } catch(Exception e){
                logger.log(Level.SEVERE,"Failed to count SkierDaySummaries",e);
            }
            try{
                int resortCount = 0;
                int sampleLimit = 100;
                for(ResortDaySummary item : resortDaySummaryTable.scan().items()){
                    resortCount++;
                    if(resortCount>=sampleLimit) break;
                }
                logger.info("ResortDaySummaries sample count: " + resortCount + " (limited to " + sampleLimit + " records)");
            } catch(Exception e){
                logger.log(Level.SEVERE,"Failed to count ResortDaySummaries",e);
            }
        } catch(Exception e){
            logger.log(Level.SEVERE,"Failed to run diagnostics",e);
        }
    }

    /**
     * Atomically increments the unique skier count and total visits in ResortDaySummaries table.
     * If the row doesn't exist, this also sets the static fields.
     *
     * @param id combined primary key "resortID#seasonID#dayID"
     * @param increment number of new unique skiers to add
     * @param totalRides number of total lift rides to add
     * @param resortID resort ID
     * @param seasonID season ID
     * @param dayID day ID
     */
    public void incrementUniqueSkierCount(String id, int increment, int totalRides, int resortID, int seasonID, int dayID) {
        // Build key for lookup
        Map<String, AttributeValue> key = Map.of(
                "id", AttributeValue.builder().s(id).build()
        );
        // Expression values for the update
        Map<String, AttributeValue> values = Map.of(
                ":inc", AttributeValue.builder().n(String.valueOf(increment)).build(),
                ":inc2", AttributeValue.builder().n(String.valueOf(totalRides)).build(),
                ":resort", AttributeValue.builder().n(String.valueOf(resortID)).build(),
                ":season", AttributeValue.builder().n(String.valueOf(seasonID)).build(),
                ":day", AttributeValue.builder().n(String.valueOf(dayID)).build()
        );

        // Update expression with SET and ADD
        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName("ResortDaySummaries")
                .key(key)
                .updateExpression(
                        "SET resortID = if_not_exists(resortID, :resort), " +
                                "seasonID = if_not_exists(seasonID, :season), " +
                                "dayID = if_not_exists(dayID, :day) " + // <== no comma
                                " ADD uniqueSkierCount :inc, totalSkierVisits :inc2"
                )
                .expressionAttributeValues(values)
                .build();

        try {
            dynamoDbClient.updateItem(request);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to increment uniqueSkierCount for id=" + id, e);
            throw e;
        }
    }
}