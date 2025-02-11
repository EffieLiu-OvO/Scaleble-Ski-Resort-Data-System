package com.cs6650.client1;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import com.google.gson.JsonObject;

public class SkiersClient {
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final String SERVER_URL = "http://Localhost:8080/CS6650-Server/skiers";
    private static final int MAX_RETRIES = 5;
    private static final int RETRY_DELAY_MS = 100;

    public static boolean sendPostRequest(JsonObject jsonBody) {
        int attempts = 0;

        while (attempts < MAX_RETRIES) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(SERVER_URL))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody.toString()))
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 201) {
                    return true;
                }

                if (response.statusCode() >= 400) {
                    attempts++;
                    if (attempts < MAX_RETRIES) {
                        System.err.println("Request failed with status " + response.statusCode()
                                + ". Attempt " + attempts + " of " + MAX_RETRIES);
                        Thread.sleep(RETRY_DELAY_MS);
                        continue;
                    }
                }
            } catch (Exception e) {
                attempts++;
                if (attempts < MAX_RETRIES) {
                    System.err.println("Request failed with exception: " + e.getMessage()
                            + ". Attempt " + attempts + " of " + MAX_RETRIES);
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

        System.err.println("Request failed after " + MAX_RETRIES + " attempts");
        return false;
    }
}