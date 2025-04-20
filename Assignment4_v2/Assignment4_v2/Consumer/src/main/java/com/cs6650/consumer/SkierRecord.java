package com.cs6650.consumer;
import java.util.HashSet;
import java.util.Set;

public class SkierRecord {
    private final int skierID;
    private int seasonID;
    private int totalLiftRides;
    private int totalVertical;
    private final Set<Integer> liftsRidden;

    public SkierRecord(int skierID, int seasonID) {
        this.skierID = skierID;
        this.seasonID = seasonID;
        this.totalLiftRides = 0;
        this.totalVertical = 0;
        this.liftsRidden = new HashSet<>();
    }

    public void addLiftRide(int liftID, int verticalGain) {
        totalLiftRides++;
        totalVertical += verticalGain;
        liftsRidden.add(liftID);
    }

    public int getSkierID() {
        return skierID;
    }

    public int getSeasonID() {
        return seasonID;
    }

    public void setSeasonID(int seasonID) {
        this.seasonID = seasonID;
    }

    public int getTotalLiftRides() {
        return totalLiftRides;
    }

    public int getTotalVertical() {
        return totalVertical;
    }

    public Set<Integer> getLiftsRidden() {
        return liftsRidden;
    }
}