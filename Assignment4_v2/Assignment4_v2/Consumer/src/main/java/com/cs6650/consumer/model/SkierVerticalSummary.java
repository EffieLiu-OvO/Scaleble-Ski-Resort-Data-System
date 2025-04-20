package com.cs6650.consumer.model;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
public class SkierVerticalSummary {
    private String id;
    private int skierID;
    private int totalVertical;
    private int seasonID;

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

    public int getSeasonID() { return seasonID; }
    public void setSeasonID(int seasonID) { this.seasonID = seasonID; }

    public int getTotalVertical() {
        return totalVertical;
    }
    public void setTotalVertical(int totalVertical) {
        this.totalVertical = totalVertical;
    }
}
