package com.cs6650.client2;

import com.google.gson.JsonObject;
import java.util.concurrent.ThreadLocalRandom;

public class LiftRideGenerator {
    public static JsonObject generateRandomLiftRide() {
        JsonObject liftRide = new JsonObject();
        liftRide.addProperty("skierID", ThreadLocalRandom.current().nextInt(1, 100001));
        liftRide.addProperty("resortID", ThreadLocalRandom.current().nextInt(1, 11));
        liftRide.addProperty("liftID", ThreadLocalRandom.current().nextInt(1, 41));
        liftRide.addProperty("seasonID", "2025");
        liftRide.addProperty("dayID", 1);
        liftRide.addProperty("time", ThreadLocalRandom.current().nextInt(1, 361));
        return liftRide;
    }
}