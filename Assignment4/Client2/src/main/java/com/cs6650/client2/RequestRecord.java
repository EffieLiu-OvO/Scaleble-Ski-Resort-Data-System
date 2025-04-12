package com.cs6650.client2;

public class RequestRecord {
    private final long startTime;
    private final String requestType;
    private final long latency;
    private final int responseCode;

    public RequestRecord(long startTime, String requestType, long latency, int responseCode) {
        this.startTime = startTime;
        this.requestType = requestType;
        this.latency = latency;
        this.responseCode = responseCode;
    }

    public long getStartTime() { return startTime; }
    public String getRequestType() { return requestType; }
    public long getLatency() { return latency; }
    public int getResponseCode() { return responseCode; }

    public String toCsvString() {
        return String.format("%d,POST,%d,%d", startTime, latency, responseCode);
    }
}