package com.cs6650.consumer.db;
import com.cs6650.consumer.model.*;
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
    public void batchUpdateSkierDaySummaries(List<SkierDaySummary> summaries){
        if(summaries.isEmpty()) return;
        try{
            Map<String,SkierDaySummary> mergedSummaries = new HashMap<>();
            for(SkierDaySummary summary : summaries){
                String id = summary.getId();
                SkierDaySummary existing = mergedSummaries.get(id);
                if(existing!=null){
                    existing.setTotalRides(existing.getTotalRides()+summary.getTotalRides());
                    existing.setTotalVertical(existing.getTotalVertical()+summary.getTotalVertical());
                    existing.getLiftsRidden().addAll(summary.getLiftsRidden());
                } else {
                    mergedSummaries.put(id,summary);
                }
            }
            for(SkierDaySummary summary : mergedSummaries.values()){
                skierDaySummaryTable.putItem(summary);
            }
            logger.info("Successfully updated " + mergedSummaries.size() + " skier day summaries (after merging)");
        } catch(Exception e){
            logger.log(Level.SEVERE,"Failed to batch update skier day summaries",e);
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
            Map<String, ResortDaySummary> mergedSummaries = new HashMap<>();

            for (ResortDaySummary summary : summaries) {
                // Ensure ID is set
                if (summary.getId() == null || summary.getId().isEmpty()) {
                    summary.setIdFromParts(summary.getResortID(), summary.getSeasonID(), summary.getDayID());
                }
                String id = summary.getId();

                ResortDaySummary existing = mergedSummaries.get(id);
                if (existing != null) {
                    existing.setUniqueSkierCount(
                            existing.getUniqueSkierCount() + summary.getUniqueSkierCount()
                    );
                } else {
                    mergedSummaries.put(id, summary);
                }
            }

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
            key.setId(resortDayId); // must set id manually
            return resortDaySummaryTable.getItem(key);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to fetch ResortDaySummary for id: " + resortDayId, e);
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

    public void incrementUniqueSkierCount(String id, int increment, int resortID, int seasonID, int dayID) {
        Map<String, AttributeValue> key = Map.of(
                "id", AttributeValue.builder().s(id).build()
        );

        Map<String, AttributeValue> values = Map.of(
                ":inc", AttributeValue.builder().n(String.valueOf(increment)).build(),
                ":resort", AttributeValue.builder().n(String.valueOf(resortID)).build(),
                ":season", AttributeValue.builder().n(String.valueOf(seasonID)).build(),
                ":day", AttributeValue.builder().n(String.valueOf(dayID)).build()
        );

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName("ResortDaySummaries")
                .key(key)
                .updateExpression(
                        "SET resortID = if_not_exists(resortID, :resort), " +
                                "    seasonID = if_not_exists(seasonID, :season), " +
                                "    dayID = if_not_exists(dayID, :day) " +
                                "ADD uniqueSkierCount :inc"
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