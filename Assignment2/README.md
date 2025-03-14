# Assignment 2 - Distributed Ski Data Processing System

## Overview
This assignment extends Assignment 1 by implementing a distributed message processing system for ski resort data. The system uses RabbitMQ for asynchronous communication between components, enhancing scalability and throughput.

### Components
1. Server
Enhanced servlet implementation that validates requests and forwards them to a RabbitMQ queue.

Validates URL paths and JSON payloads
Uses a channel pool to efficiently communicate with RabbitMQ
Returns appropriate HTTP status codes to clients

2. Client2
Optimized multithreaded client for performance testing.

Configurable thread counts and batch sizes
Collects detailed performance metrics
Supports two-phase load testing

Note: When running locally, you must change the server URL in SkiersClient.java:
 
 ```bash
For load balancer testing: http://skier-target-group-1803267666.us-west-2.elb.amazonaws.com/CS6650-Server/skiers
For single instance testing: Use the IP address of a specific server instance
```

3. Consumer
Multithreaded application that processes messages from RabbitMQ.

Maintains thread-safe records of skier activities
Configurable number of consumer threads
Uses prefetching to optimize message retrieval

### Setup Instructions
1. RabbitMQ:

Deploy on a dedicated EC2 instance
Enable management console for monitoring

2. Server:

Deploy on one or more EC2 instances
Configure to connect to the RabbitMQ instance
For load balancing, deploy multiple instances and configure with AWS ELB

3. Consumer:

Deploy on a separate EC2 instance
Configure environment variables to connect to RabbitMQ:

RABBITMQ_HOST
RABBITMQ_USERNAME
RABBITMQ_PASSWORD

4. Client:

Run locally or on EC2 instance
Update server URL to point to load balancer or individual server instance


## Testing

Important: All testing should be performed using the Client2 application, which has been optimized for this assignment with proper metrics collection and reporting.
The system should be tested with various configurations to find optimal settings for:

Client thread counts (adjust INITIAL_THREAD_COUNT and PHASE2_THREAD_COUNT)
Consumer thread counts
RabbitMQ queue management
Server connection pool sizes

Monitor queue length in RabbitMQ management console to ensure it stays below 1000 messages. The test results should compare performance between single-instance deployment and load-balanced deployment.
 
