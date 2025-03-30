package com.cs6650.consumer.model;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import java.util.HashSet;
import java.util.Set;

@DynamoDbBean
public class ResortDaySummary {
    private String id; //resortID#dayID
    private int resortID;
    private int dayID;
    private Set<Integer> uniqueSkiers;

    public ResortDaySummary() {
        this.uniqueSkiers = new HashSet<>();
    }

    @DynamoDbPartitionKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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