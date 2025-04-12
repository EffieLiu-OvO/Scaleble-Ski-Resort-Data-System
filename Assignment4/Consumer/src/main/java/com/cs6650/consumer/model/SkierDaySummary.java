package com.cs6650.consumer.model;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import java.util.HashSet;
import java.util.Set;

@DynamoDbBean
public class SkierDaySummary {
    private String id; // skierID#dayID
    private int skierID;
    private int dayID;
    private int resortID;
    private int totalVertical;
    private int totalRides;
    private Set<Integer> liftsRidden;

    public SkierDaySummary() {
        this.liftsRidden = new HashSet<>();
    }

    @DynamoDbPartitionKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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