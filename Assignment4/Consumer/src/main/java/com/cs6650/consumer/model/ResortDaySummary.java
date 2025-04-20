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
    private Set<Integer> uniqueSkiers; // Set of skier IDs that visited on this day
    private int seasonID; // for season id
    public ResortDaySummary() {
        this.uniqueSkiers = new HashSet<>();
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

    public int getResortID() {
        return resortID;
    }

    public void setResortID(int resortID) {
        this.resortID = resortID;
    }

    public int getDayID() {
        return dayID;
    }

    public void setDayID(int dayID) {
        this.dayID = dayID;
    }

    public int getSeasonID() { return seasonID; }
    public void setSeasonID(int seasonID) { this.seasonID = seasonID; }

    public Set<Integer> getUniqueSkiers() {
        return uniqueSkiers;
    }

    public void setUniqueSkiers(Set<Integer> uniqueSkiers) {
        this.uniqueSkiers = uniqueSkiers;
    }

    public int getUniqueSkierCount() {
        return uniqueSkiers.size();
    }
}