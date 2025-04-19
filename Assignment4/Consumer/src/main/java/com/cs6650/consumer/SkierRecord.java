package com.cs6650.consumer;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class SkierRecord {
    private final int skierID;
    private final AtomicInteger totalLiftRides;
    private final AtomicInteger totalVertical;
    private final Set<Integer> liftsRidden = ConcurrentHashMap.newKeySet();

    // season ID
    private int seasonID;

    public SkierRecord(int skierID, int seasonID) {
        this.skierID = skierID;
        this.totalLiftRides = new AtomicInteger(0);
        this.totalVertical = new AtomicInteger(0);
        this.seasonID = seasonID;
    }

    public void addLiftRide(int liftID, int vertical) {
        totalLiftRides.incrementAndGet();
        totalVertical.addAndGet(vertical);
    }

    public int getSkierID() {
        return skierID;
    }

    public int getSeasonID() {return seasonID; }

    public void setSeasonID(int seasonID ) {
        this.seasonID = seasonID;
    }

    public int getTotalLiftRides() {
        return totalLiftRides.get();
    }

    public int getTotalVertical() {
        return totalVertical.get();
    }

    /**
     * Returns an unmodifiable view of the lifts ridden so far
     */
    public Set<Integer> getLiftsRidden() {
        return Collections.unmodifiableSet(liftsRidden);
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