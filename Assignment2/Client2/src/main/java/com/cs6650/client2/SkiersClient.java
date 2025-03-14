package com.cs6650.client2;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import com.google.gson.JsonObject;

public class SkiersClient {
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final String SERVER_URL = "http://skier-target-group-1803267666.us-west-2.elb.amazonaws.com/CS6650-Server/skiers";
    private static final int MAX_RETRIES = 5;
    private static final int RETRY_DELAY_MS = 100;
    private final List<RequestRecord> records;

    public SkiersClient(List<RequestRecord> records) {
        this.records = records;
    }

    public boolean sendPostRequest(JsonObject jsonBody) {
        int attempts = 0;
        long startTime = System.nanoTime();

        while (attempts < MAX_RETRIES) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(SERVER_URL))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody.toString()))
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                long endTime = System.nanoTime();
                long latency = (endTime - startTime) / 1_000_000;

                synchronized (records) {
                    records.add(new RequestRecord(
                            startTime / 1_000_000,
                            "POST",
                            Math.max(1, latency),
                            response.statusCode()
                    ));
                }

                if (response.statusCode() == 201) {
                    return true;
                }

                if (response.statusCode() >= 400) {
                    attempts++;
                    if (attempts < MAX_RETRIES) {
                        try {
                            Thread.sleep(RETRY_DELAY_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        continue;
                    }
                }

            } catch (Exception e) {
                attempts++;
                if (attempts < MAX_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    continue;
                }
            }
        }
        return false;
    }
}