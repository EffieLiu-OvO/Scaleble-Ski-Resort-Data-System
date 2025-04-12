package com.cs6650.consumer.test;

import com.cs6650.consumer.model.SkierLiftRide;
import com.cs6650.consumer.model.SkierDaySummary;
import com.cs6650.consumer.model.ResortDaySummary;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple standalone test class to verify DynamoDB connection and table creation
 */
public class DynamoDBConnectionTest {
    private static final Logger logger = Logger.getLogger(DynamoDBConnectionTest.class.getName());

    public static void main(String[] args) {
        // Initialize DynamoDB client for local testing
        DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.US_WEST_2)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy", "dummy")))
                .build();

        try {
            // Test connection by listing tables
            logger.info("Testing DynamoDB connection...");
            ListTablesResponse listTablesResponse = dynamoDbClient.listTables();
            logger.info("Connection successful! Existing tables: " + listTablesResponse.tableNames());

            // Create enhanced client
            DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                    .dynamoDbClient(dynamoDbClient)
                    .build();

            // Define table references
            DynamoDbTable<SkierLiftRide> liftRideTable = enhancedClient.table("SkierLiftRides",
                    TableSchema.fromBean(SkierLiftRide.class));
            DynamoDbTable<SkierDaySummary> skierDaySummaryTable = enhancedClient.table("SkierDaySummaries",
                    TableSchema.fromBean(SkierDaySummary.class));
            DynamoDbTable<ResortDaySummary> resortDaySummaryTable = enhancedClient.table("ResortDaySummaries",
                    TableSchema.fromBean(ResortDaySummary.class));

            // Try to create tables
            try {
                logger.info("Attempting to create SkierLiftRides table...");
                liftRideTable.createTable(builder -> builder
                        .provisionedThroughput(b -> b
                                .readCapacityUnits(100L)
                                .writeCapacityUnits(500L)
                                .build())
                );
                logger.info("Successfully created SkierLiftRides table");
            } catch (ResourceInUseException e) {
                logger.info("SkierLiftRides table already exists");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to create SkierLiftRides table", e);
            }

            try {
                logger.info("Attempting to create SkierDaySummaries table...");
                skierDaySummaryTable.createTable(builder -> builder
                        .provisionedThroughput(b -> b
                                .readCapacityUnits(50L)
                                .writeCapacityUnits(100L)
                                .build())
                );
                logger.info("Successfully created SkierDaySummaries table");
            } catch (ResourceInUseException e) {
                logger.info("SkierDaySummaries table already exists");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to create SkierDaySummaries table", e);
            }

            try {
                logger.info("Attempting to create ResortDaySummaries table...");
                resortDaySummaryTable.createTable(builder -> builder
                        .provisionedThroughput(b -> b
                                .readCapacityUnits(50L)
                                .writeCapacityUnits(100L)
                                .build())
                );
                logger.info("Successfully created ResortDaySummaries table");
            } catch (ResourceInUseException e) {
                logger.info("ResortDaySummaries table already exists");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to create ResortDaySummaries table", e);
            }

            // Test write operation with a sample record
            logger.info("Testing write operation with a sample record...");
            SkierLiftRide testRide = new SkierLiftRide();
            testRide.setId("test-123");
            testRide.setSkierID(123);
            testRide.setResortID(1);
            testRide.setDayID(1);
            testRide.setLiftID(10);
            testRide.setTime(900);
            testRide.setVertical(100);

            liftRideTable.putItem(testRide);
            logger.info("Successfully wrote test record to SkierLiftRides table");

            // List tables again to confirm creation
            listTablesResponse = dynamoDbClient.listTables();
            logger.info("Final table list: " + listTablesResponse.tableNames());

            logger.info("DynamoDB connection and table creation test completed successfully!");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Test failed with exception", e);
            e.printStackTrace();
        }
    }
}