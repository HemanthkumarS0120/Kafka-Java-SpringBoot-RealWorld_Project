# Kafka Java Spring Boot Real World Project — Wikimedia Event Pipeline


A real-world multi-module Spring Boot application that streams live Wikimedia recent-change events from the Wikimedia EventStream API, publishes them to a Kafka topic, and consumes + persists them to a database. Demonstrates a complete end-to-end Kafka producer/consumer pipeline.


---


## Table of Contents


- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Architecture Overview](#architecture-overview)
- [Modules](#modules)
- [How It Works](#how-it-works)
- [Configuration](#configuration)
- [Getting Started](#getting-started)
- [Running the Application](#running-the-application)


---


## Tech Stack


| Technology        | Version          |
|-------------------|------------------|
| Java              | 21               |
| Spring Boot       | 3.4.6            |
| Spring Kafka      | -                |
| Spring Data JPA   | -                |
| Lombok            | -                |
| LaunchDarkly EventSource | -        |
| MySQL / H2        | -                |
| Maven (multi-module) | Wrapper included |


---


## Project Structure


```
Kafka-Java-SpringBoot-RealWorld_Project/
├── pom.xml                                        # Parent multi-module POM
├── kafka-wikimedia-producer/
│   └── src/main/java/com/springboot/kafka_wikimedia_producer/
│       ├── KafkaTopicConfig.java                  # Defines Kafka topic bean
│       ├── WikimediaChangesProducer.java          # Connects to Wikimedia stream & publishes
│       ├── WikimediaChangesHandler.java           # EventSource event handler
│       └── KafkaWikimediaProducerApplication.java # Producer app entry point
└── kafka-wikimedia-consumer-database/
    └── src/main/java/com/springboot/kafka_wikimedia_consumer/
        ├── KafkaDatabaseConsumer.java             # Kafka listener — saves events to DB
        ├── KafkaWikimediaConsumerApplication.java # Consumer app entry point
        ├── entity/
        │   └── WikimediaData.java                 # JPA entity for persisted events
        └── repository/
            └── WikimediaDataRepository.java       # Spring Data JPA repository
```


---


## Architecture Overview


```
Wikimedia EventStream API
  (https://stream.wikimedia.org/v2/stream/recentchange)
          │
          │  Server-Sent Events (SSE)
          ▼
┌─────────────────────────────┐
│  WikimediaChangesProducer   │   ← Runs for 10 minutes, streams events
│  (kafka-wikimedia-producer) │
└──────────────┬──────────────┘
               │  publishes to Kafka topic
               ▼
        ┌─────────────┐
        │ Kafka Topic │
        └──────┬──────┘
               │  consumed by
               ▼
┌──────────────────────────────────────┐
│  KafkaDatabaseConsumer               │   ← @KafkaListener
│  (kafka-wikimedia-consumer-database) │
└──────────────┬───────────────────────┘
               │  saves to DB
               ▼
        ┌─────────────┐
        │  Database   │   (WikimediaData table)
        └─────────────┘
```


---


## Modules


### kafka-wikimedia-producer


| Class                       | Responsibility                                               |
|-----------------------------|--------------------------------------------------------------|
| `WikimediaChangesProducer`  | Opens SSE connection to Wikimedia, publishes each event to Kafka |
| `WikimediaChangesHandler`   | LaunchDarkly EventSource handler — receives events from the stream |
| `KafkaTopicConfig`          | Creates the Kafka `NewTopic` bean on startup                 |


**Key behavior:** `WikimediaChangesProducer.sendMessage()` connects to the Wikimedia real-time stream using a `BackgroundEventSource`, streams for 10 minutes, and publishes each raw event as a string to the configured Kafka topic.


---


### kafka-wikimedia-consumer-database


| Class                     | Responsibility                                               |
|---------------------------|--------------------------------------------------------------|
| `KafkaDatabaseConsumer`   | `@KafkaListener` — receives events, creates `WikimediaData` entities, saves to DB |
| `WikimediaData`           | JPA entity representing a persisted Wikimedia event          |
| `WikimediaDataRepository` | Spring Data JPA repository for `WikimediaData`              |


---


## How It Works


1. The **producer** starts and opens an SSE connection to `https://stream.wikimedia.org/v2/stream/recentchange`.
2. Every real-time change event (Wikipedia edits, new pages, etc.) is received by `WikimediaChangesHandler`.
3. The handler passes each event string to `WikimediaChangesProducer`, which publishes it to a Kafka topic.
4. The **consumer** application listens on the same Kafka topic via `@KafkaListener`.
5. Each consumed event is wrapped in a `WikimediaData` entity and saved to the database.


---


## Configuration


Add the following to `application.properties` in both modules:


**Producer** (`kafka-wikimedia-producer/src/main/resources/application.properties`):
```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.topic.name=wikimedia_recentchange
```


**Consumer** (`kafka-wikimedia-consumer-database/src/main/resources/application.properties`):
```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.topic.name=wikimedia_recentchange
spring.kafka.consumer.group-id=wikimedia_consumer_group
spring.kafka.consumer.auto-offset-reset=earliest


# Database
spring.datasource.url=jdbc:mysql://localhost:3306/wikimedia_db
spring.datasource.username=your_username
spring.datasource.password=your_password
spring.jpa.hibernate.ddl-auto=update
```


---


## Getting Started


### Prerequisites


- Java 21+
- Apache Kafka running locally (port `9092`)
- MySQL (or H2 for in-memory)
- Maven


### Start Kafka (Docker)


```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```


### Clone the Repository


```bash
git clone https://github.com/HemanthkumarS0120/Kafka-Java-SpringBoot-RealWorld_Project.git
cd Kafka-Java-SpringBoot-RealWorld_Project
```


---


## Running the Application


Start both modules independently in separate terminals:


**Consumer first** (so no events are missed):
```bash
cd kafka-wikimedia-consumer-database
./mvnw spring-boot:run
```


**Producer second:**
```bash
cd kafka-wikimedia-producer
./mvnw spring-boot:run
```


> The producer streams Wikimedia events for **10 minutes** then stops. The consumer continues running and persists all received events to the database.


On Windows, replace `./mvnw` with `mvnw.cmd`.



