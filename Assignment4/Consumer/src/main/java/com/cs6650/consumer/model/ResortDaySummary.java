package com.cs6650.consumer.model;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a summary of skier visits for a given resort on a specific day of a season.
 *
 * This model is mapped to the DynamoDB table "ResortDaySummaries" using the Enhanced Client API.
 *
 * The primary key is a composite string: "resortID#seasonID#dayID".
 * It stores the total number of unique skiers and total ride events for that day.
 */
@DynamoDbBean
public class ResortDaySummary {
    // Composite key for DynamoDB: resortID#seasonID#dayID
    private String id; // resortID#seasonID#dayID
    private int resortID;
    private int dayID;
    // Number of unique skiers who visited on this day
    private int uniqueSkierCount; // Set of skier IDs that visited on this day
    private int seasonID; // for season id

    // Total number of lift rides taken by all skiers on this day
    private int totalSkierVisits;


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

    @DynamoDbAttribute("totalSkierVisits")
    public int getTotalSkierVisits() {
        return totalSkierVisits;
    }

    public void setTotalSkierVisits(int totalSkierVisits) {
        this.totalSkierVisits = totalSkierVisits;
    }
}