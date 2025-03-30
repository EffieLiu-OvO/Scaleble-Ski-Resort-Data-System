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
public class DynamoDBManager {
    private static final Logger logger = Logger.getLogger(DynamoDBManager.class.getName());
    private final DynamoDbClient dynamoDbClient;
    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<SkierLiftRide> liftRideTable;
    private final DynamoDbTable<SkierDaySummary> skierDaySummaryTable;
    private final DynamoDbTable<ResortDaySummary> resortDaySummaryTable;
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
    public void createTablesIfNotExist(){
        try{
            liftRideTable.createTable(builder->builder.provisionedThroughput(b->b.readCapacityUnits(100L).writeCapacityUnits(2000L).build()));
            logger.info("Successfully created SkierLiftRides table");
            skierDaySummaryTable.createTable(builder->builder.provisionedThroughput(b->b.readCapacityUnits(50L).writeCapacityUnits(200L).build()));
            logger.info("Successfully created SkierDaySummaries table");
            resortDaySummaryTable.createTable(builder->builder.provisionedThroughput(b->b.readCapacityUnits(50L).writeCapacityUnits(200L).build()));
            logger.info("Successfully created ResortDaySummaries table");
        } catch(ResourceInUseException e){
            logger.info("Tables already exist, continuing");
        } catch(Exception e){
            logger.log(Level.SEVERE,"Error creating tables",e);
            throw new RuntimeException("Failed to create DynamoDB tables",e);
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
            logger.info(String.format("Batch save of %d items completed in %d ms (%.2f items/second)", rides.size(), duration, (rides.size()*1000.0)/duration));
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
    public void batchUpdateResortDaySummaries(List<ResortDaySummary> summaries){
        if(summaries.isEmpty()) return;
        try{
            Map<String,ResortDaySummary> mergedSummaries = new HashMap<>();
            for(ResortDaySummary summary : summaries){
                String id = summary.getId();
                ResortDaySummary existing = mergedSummaries.get(id);
                if(existing!=null){
                    existing.getUniqueSkiers().addAll(summary.getUniqueSkiers());
                } else {
                    mergedSummaries.put(id,summary);
                }
            }
            for(ResortDaySummary summary : mergedSummaries.values()){
                resortDaySummaryTable.putItem(summary);
            }
            logger.info("Successfully updated " + mergedSummaries.size() + " resort day summaries (after merging)");
        } catch(Exception e){
            logger.log(Level.SEVERE,"Failed to batch update resort day summaries",e);
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
}
