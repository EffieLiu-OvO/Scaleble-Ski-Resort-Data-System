# CS6650 Assignments Repository

## Overview
This repository contains all assignments for CS6650 - Building Scalable Distributed Systems. Each assignment focuses on building components of a distributed system for a ski resort data collection platform.

## Repository Structure
Each assignment is contained in its own directory:

### /Assignment1
Implementation of a multithreaded client and server for processing ski lift usage data.
- `server/`: Server implementation with servlet
- `client1/`: Basic client implementation
- `client2/`: Enhanced client with performance monitoring

### /Assignment2
Implementation of a distributed system with message queuing for asynchronous processing of ski data.
* `server/`: Enhanced server with RabbitMQ integration and request validation
* `client2/`: Optimized client with performance tuning
* `consumer/`: Multithreaded RabbitMQ consumer application

### /Assignment3
Implementation of database persistence layer to store ski lift usage data.
* `consumer/`: Enhanced consumer with DynamoDB integration
* `models/`: Data models for database representation
* `documentation/`: Performance test results and database design documentation

### /Assignment4
Implementation of three GET endpoints for querying skier vertical totals, skier day summaries, and resort day summaries.
Includes performance testing using Apache JMeter with 128 threads and 500 iterations.
This assignment was completed in collaboration with team members, with each member

## Running the Projects
Each assignment directory contains its own README with specific instructions on how to set up and run the components.

## Course Information
- Course: CS6650 - Building Scalable Distributed Systems
- Term: Spring 2025
- Institution: Northeastern University, Seattle
