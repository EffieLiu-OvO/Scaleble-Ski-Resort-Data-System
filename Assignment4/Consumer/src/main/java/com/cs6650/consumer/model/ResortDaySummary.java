package com.cs6650.consumer.model;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a summary of unique skier visits for a specific resort, season, and day.
 *
 * Mapped to the DynamoDB table \"ResortDaySummaries\".
 *
 * Composite key format: "resortID#seasonID#dayID"
 */
@DynamoDbBean
public class ResortDaySummary {
    //    private String id; //resortID#dayID
    private String id; // resortID#seasonID#dayID
    private int resortID;
    private int dayID;
    private int uniqueSkierCount; // Set of skier IDs that visited on this day
    private int seasonID; // for season id

    public ResortDaySummary() {
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("id")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * Helper to set ID based on resortID, seasonID, and dayID.
     * Format: "resortID#seasonID#dayID"
     */
    public void setIdFromParts(int resortID, int seasonID, int dayID) {
        this.id = resortID + "#" + seasonID + "#" + dayID;
    }

    @DynamoDbAttribute("resortID")
    public int getResortID() {
        return resortID;
    }

    public void setResortID(int resortID) {
        this.resortID = resortID;
    }

    @DynamoDbAttribute("dayID")
    public int getDayID() {
        return dayID;
    }

    public void setDayID(int dayID) {
        this.dayID = dayID;
    }

    @DynamoDbAttribute("seasonID")
    public int getSeasonID() { return seasonID; }
    public void setSeasonID(int seasonID) { this.seasonID = seasonID; }

    @DynamoDbAttribute("uniqueSkierCount")
    public int getUniqueSkierCount() { return uniqueSkierCount; }

    public void setUniqueSkierCount(int uniqueSkierCount) {
        this.uniqueSkierCount = uniqueSkierCount;
    }
}