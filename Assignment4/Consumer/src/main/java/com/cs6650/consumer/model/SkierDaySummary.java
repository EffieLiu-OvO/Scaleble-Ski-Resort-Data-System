package com.cs6650.consumer.model;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a summary of a skier's activity for a specific day at a specific resort and season.
 * Mapped to the DynamoDB table "SkierDaySummaries".
 *
 * Composite key format: "skierID#resortID#seasonID#dayID"
 */
@DynamoDbBean
public class SkierDaySummary {
    private String id; // skierID#resortID#seasonID#dayID
    private int skierID;
    private int dayID;
    private int resortID;
    private int seasonID;
    private int totalVertical;
    private int totalRides;
    private Set<Integer> liftsRidden;
    
    public SkierDaySummary() {
        this.liftsRidden = new HashSet<>();
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
     * Helper to set ID based on skierID, resortID, seasonID, and dayID.
     * Format: "skierID#resortID#seasonID#dayID"
     */
    public void setIdFromParts(int skierID, int resortID, int seasonID, int dayID) {
        this.id = skierID + "#" + resortID + "#" + seasonID + "#" + dayID;
    }
    
    public int getSkierID() {
        return skierID;
    }
    
    public void setSkierID(int skierID) {
        this.skierID = skierID;
    }
    
    public int getDayID() {
        return dayID;
    }
    
    public void setDayID(int dayID) {
        this.dayID = dayID;
    }
    
    public int getResortID() {
        return resortID;
    }
    
    public void setResortID(int resortID) {
        this.resortID = resortID;
    }
    
    public int getSeasonID() {
        return seasonID;
    }
    
    public void setSeasonID(int seasonID) {
        this.seasonID = seasonID;
    }
    
    
    public int getTotalVertical() {
        return totalVertical;
    }
    
    public void setTotalVertical(int totalVertical) {
        this.totalVertical = totalVertical;
    }
    
    public int getTotalRides() {
        return totalRides;
    }
    
    public void setTotalRides(int totalRides) {
        this.totalRides = totalRides;
    }
    
    public Set<Integer> getLiftsRidden() {
        return liftsRidden;
    }
    
    public void setLiftsRidden(Set<Integer> liftsRidden) {
        this.liftsRidden = liftsRidden;
    }
}