# Client 1 - Basic Ski Resort Data Client  

## Setup and Configuration  

### URL Configuration  
To change the server URL, modify the `SERVER_URL` constant in `SkiersClient.java`:  

```java
private static final String SERVER_URL = "http://your-ec2-ip:8080/CS6650-Server/skiers";
```

## Client Configuration Settings

Main settings can be found in MultithreadedClient.java:

 ```bash
INITIAL_THREAD_COUNT: Initial number of threads (default: 32);
REQUESTS_PER_THREAD: Requests per thread (default: 1000);
TOTAL_REQUESTS: Total number of requests to send (default: 200000);
PHASE2_THREAD_COUNT: Number of threads for phase 2 (default: 32);
QUEUE_CAPACITY: Event queue size (default: 10000);
```

## How to Run

1. Ensure correct SERVER_URL in SkiersClient.java
2. Run MultithreadedClient.java
3. The client will output:
``` bash
Client configuration
Total successful/failed requests
Total run time
Throughput (requests/second)
```

# Client 2 - Enhanced Ski Resort Data Client with Performance Monitoring
## Setup and Configuration  

### URL Configuration  
To change the server URL, modify the `SERVER_URL` constant in `SkiersClient.java`:  

```java
private static final String SERVER_URL = "http://your-ec2-ip:8080/CS6650-Server/skiers";
```

## Client Configuration Settings

Main settings can be found in MultithreadedClient.java:

 ```bash
INITIAL_THREAD_COUNT: Initial number of threads (default: 32)
REQUESTS_PER_THREAD: Requests per thread (default: 1000)
TOTAL_REQUESTS: Total number of requests to send (default: 200000)
PHASE2_THREAD_COUNT: Number of threads for phase 2 (default: 32)
QUEUE_CAPACITY: Event queue size (default: 10000)
 ```

## How to Run

1. Ensure correct SERVER_URL in SkiersClient.java
2. Run MultithreadedClient.java
3. The client will output:
 ```bash
Client configuration
Total successful/failed requests
Total run time
Throughput (requests/second)

Performance statistics
Mean response time
Median response time
p99 response time
Min/Max response times
 ```

## Generated Files
 ```bash
request_records.csv: Contains detailed metrics for each request
throughput_plot.png (please run this): Visual representation of throughput over time
 ```
