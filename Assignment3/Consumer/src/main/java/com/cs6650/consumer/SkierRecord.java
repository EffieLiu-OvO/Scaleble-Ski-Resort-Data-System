package com.cs6650.consumer;

import java.util.concurrent.atomic.AtomicInteger;

public class SkierRecord {
    private final int skierID;
    private final AtomicInteger totalLiftRides;
    private final AtomicInteger totalVertical;

    public SkierRecord(int skierID) {
        this.skierID = skierID;
        this.totalLiftRides = new AtomicInteger(0);
        this.totalVertical = new AtomicInteger(0);
    }

    public void addLiftRide(int liftID, int vertical) {
        totalLiftRides.incrementAndGet();
        totalVertical.addAndGet(vertical);
    }

    public int getSkierID() {
        return skierID;
    }

    public int getTotalLiftRides() {
        return totalLiftRides.get();
    }

    public int getTotalVertical() {
        return totalVertical.get();
    }

    @Override
    public String toString() {
        return "SkierRecord{" +
                "skierID=" + skierID +
                ", totalLiftRides=" + totalLiftRides +
                ", totalVertical=" + totalVertical +
                '}';
    }
}