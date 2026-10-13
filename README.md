# Kafka Project 
## 1.Add Spring Kafka

I added the Spring for Apache Kafka dependency to my Spring Boot application.

Why?
It provides Spring integration with Kafka, including KafkaTemplate for producing messages and @KafkaListener for consuming messages.

## 2.Run Kafka using Docker

I used Docker to run Kafka locally instead of installing Kafka directly on my machine.

Why?
Docker makes it easier to start, stop and manage Kafka.

Kafka is running on:

localhost:9092


## 3.Create Kafka Topic

I created a topic called:

employee-events

with 2 partitions.

Topic: A topic is a logical category where Kafka stores messages.

Partition: A topic can be divided into multiple partitions for scalability and parallel processing.

## 4.Configure Kafka Producer

I configured Spring Boot with the Kafka broker address:

localhost:9092

Then I used KafkaTemplate to send messages to Kafka.

Producer: A producer is responsible for publishing messages/events to a Kafka topic.

## 5.Integrate REST API with Kafka

I created a REST API through which the client sends employee information.

The flow is:

REST API
   ↓
Spring Boot
   ↓
Kafka Producer
   ↓
Kafka Topic


## 6.Create Employee Event

Instead of sending simple strings, I created an EmployeeEvent Java object containing:

employeeId
name
department

For example:

{
  "employeeId": 101,
  "name": "Rahul",
  "department": "IT"
}

This represents the event that needs to be published to Kafka.


## 7.JSON Serialization

I configured JacksonJsonSerializer.

Serialization means converting a Java object into a format that can be sent through Kafka.

EmployeeEvent
      ↓
JSON Serialization
      ↓
Kafka

So the overall producer-side flow is:

Client
  ↓
REST API
  ↓
EmployeeEvent
  ↓
KafkaTemplate
  ↓
JSON Serialization
  ↓
Kafka Topic



## 8.Implement Kafka Consumer

After producing the event to Kafka, I implemented a Kafka Consumer using Spring Kafka's @KafkaListener.

Why?
The producer sends messages to Kafka, while the consumer reads and processes those messages from the Kafka topic.

The flow is:

Kafka Topic
     ↓
Kafka Consumer
     ↓
Process EmployeeEvent

I configured the consumer with a consumer group called employee-group.

Consumer Group: A group of consumers that work together to consume messages from Kafka.

## 9.JSON Deserialization

Since the producer sends the EmployeeEvent as JSON, the consumer needs to convert that JSON back into a Java object.

I configured JacksonJsonDeserializer.

Deserialization means converting the received JSON data back into a Java object.

Kafka
   ↓
JSON
   ↓
JacksonJsonDeserializer
   ↓
EmployeeEvent

So the consumer-side flow is:

Kafka Topic
     ↓
JSON Message
     ↓
JSON Deserialization
     ↓
EmployeeEvent
     ↓
Kafka Consumer


## 10.Kafka Partitions

I created the employee-events topic with 2 partitions.

A partition is an ordered sequence of messages inside a Kafka topic.

employee-events

Partition 0
-----------
Message 1
Message 3
Message 5

Partition 1
-----------
Message 2
Message 4
Message 6

Why?
Partitions allow Kafka to distribute messages and provide parallelism and scalability.

## 11.Consumer Group

I configured my consumer with:

employee-group

Consumers belonging to the same consumer group can share the partitions of a topic.

For example:

2 Partitions
      ↓
Consumer 1 → Partition 0
Consumer 2 → Partition 1

Important: Within the same consumer group, one partition is assigned to only one consumer at a time.

## 12.Kafka Offset

Kafka maintains an offset for each message within a partition.

For example:

Partition 0

Offset 0 → Message 1
Offset 1 → Message 2
Offset 2 → Message 3

Why?
The offset allows Kafka to keep track of the consumer's progress.

In simple terms:

Offset tells Kafka where the consumer is in the partition.

## 13.Acknowledgement and Offset Management

After a consumer receives a message, we need to consider whether the message was actually processed successfully.

The basic flow is:

Kafka Message
     ↓
Consumer
     ↓
Process Message
     ↓
Successful Processing
     ↓
Acknowledgement


## 14.ExecutorService for Parallel Processing

To process multiple Kafka events concurrently, I used Java's ExecutorService.

I configured a fixed thread pool with 3 worker threads.

ExecutorService
      |
      ├── Worker 1
      ├── Worker 2
      └── Worker 3

Why?

Instead of processing every event sequentially:

Event 1 → Complete
Event 2 → Complete
Event 3 → Complete

multiple events can be processed concurrently:

Event 1 → Worker 1
Event 2 → Worker 2
Event 3 → Worker 3



## 15.Kafka Parallelism vs Application Thread Parallelism

These are two different concepts.

Kafka-level parallelism is achieved using:

Partitions

Application-level parallelism is achieved using:

ExecutorService
Worker Threads

So my architecture becomes:

Kafka Topic
     ↓
Partitions
     ↓
@KafkaListener
     ↓
ExecutorService
     ↓
Worker 1 | Worker 2 | Worker 3

This distinction is important because having 3 worker threads does not mean Kafka automatically creates 3 partitions or 3 consumers.


## 16.Important Issue with Asynchronous Processing

When using ExecutorService, the Kafka listener can submit a task to a worker and return before the worker has finished processing.

Kafka Message
     ↓
@KafkaListener
     ↓
ExecutorService.submit()
     ↓
Listener returns
     ↓
Worker still processing

This creates an important offset/acknowledgement problem.

If the offset is committed before the worker successfully completes the processing, an application failure could cause the message to be considered processed even though the actual business processing was not completed.
     ↓
Offset Commit

Why is this important?

If the application fails before processing is completed, we don't want Kafka to incorrectly consider the message successfully processed.


## 17.Consumer Idempotency

Consumer idempotency means processing the same event multiple times produces the same business result without creating duplicate effects.

Why?

In Kafka, the same message may be delivered more than once. For example, a consumer might update a database successfully but fail before committing the Kafka offset. When the consumer restarts, Kafka may deliver the same message again.

Without idempotency, the application could perform the same business operation twice.

Example:

Suppose an employee event contains:

{
  "eventId": "EVT101",
  "employeeId": 101,
  "name": "Rahul"
}

If Kafka delivers this event twice, the application checks whether eventId = EVT101 has already been processed.

If it is new, the application processes the event and records its ID.

If it has already been processed, the application skips the duplicate business operation.

Interview answer:

I would implement consumer idempotency by assigning a unique event ID to each message and maintaining a record of processed event IDs. Before processing a message, the consumer checks whether that event has already been handled. This prevents duplicate business effects when Kafka redelivers a message.


## 18.Transactional Outbox Pattern

The Transactional Outbox Pattern helps ensure reliable event publishing when an application updates a database and publishes an event to Kafka.

Why?

Suppose an application saves employee information in a database and then sends an event to Kafka.

Two situations can occur:

The database transaction succeeds, but Kafka publishing fails.

Kafka receives the event, but the database transaction fails.

This can cause inconsistency between the database and Kafka.

How does the outbox pattern work?

Application
     ↓
Database Transaction
     ├── Save Employee
     └── Save Event in Outbox Table
                  ↓
          Outbox Publisher
                  ↓
              Kafka Topic
                  ↓
               Consumer

The employee record and outbox event are saved in the same database transaction. A separate publisher reads pending outbox events and publishes them to Kafka.

Interview answer:

I would use the Transactional Outbox Pattern to solve the dual-write problem between a database and Kafka. The business data and event are saved in the same database transaction. A separate publisher then sends the event to Kafka. This prevents the event from being lost simply because Kafka was unavailable during the database transaction.

Important: The outbox publisher may publish an event more than once, so consumer idempotency is still important.

## 19.Schema Evolution

Schema evolution means changing an event's structure over time while maintaining compatibility between producers and consumers.

Why?

Initially, an employee event might contain:

{
  "employeeId": 101,
  "name": "Rahul"
}

Later, the application may need to add a department field.

{
  "employeeId": 101,
  "name": "Rahul",
  "department": "IT"
}

If producers and consumers are updated independently, changes to the event structure could cause compatibility problems.

A schema management approach defines which changes are allowed and how different versions remain compatible. Tools such as Apache Avro and Schema Registry are commonly used for this.

Interview answer:

I would manage schema evolution by defining an explicit event schema and compatibility rules. When adding or changing fields, I would ensure that existing consumers can continue to read messages produced by newer versions. This allows producers and consumers to evolve independently.

## 20.Observability in Kafka

Observability means understanding an application's internal behavior using logs, metrics, and traces.

Why?

In a production Kafka application, we need to identify processing failures, growing consumer lag, repeated retries, and publishing problems.

Three important areas are:

Logs: Show which event was processed and why processing failed.

Metrics: Show consumer lag, processing throughput, error counts, and processing latency.

Tracing: Helps follow an event across multiple services.

For example, if consumer lag keeps increasing, the consumer might be processing messages more slowly than the producer is publishing them.

Interview answer:

I would implement observability using structured logs, application metrics, and distributed tracing where needed. I would monitor consumer lag, processing latency, retry counts, and failure rates. This helps identify bottlenecks and investigate production issues.

## 21.Message Delivery Guarantees

Kafka applications commonly discuss three delivery approaches.

Approach

Meaning

At-most-once

A message is processed zero or one time; a failure can cause message loss.

At-least-once

A message is processed one or more times; duplicates are possible.

Exactly-once

Prevents duplicate effects within a supported transactional processing boundary, subject to the system's guarantees.

In your project, manual acknowledgment, retries, and a DLT help with reliable processing. However, they do not by themselves guarantee exactly-once business processing.

For database updates or external API calls, idempotency is still important.

Interview answer:

My current implementation uses manual acknowledgment, retries, and a Dead Letter Topic to handle failures. I understand that these mechanisms do not automatically guarantee exactly-once business effects. I would add consumer idempotency and, where appropriate, transactional processing to strengthen reliability.

## 22.Final Architecture — Production-Oriented Kafka Pipeline

In this project currently includes the producer, Kafka topic with two partitions, consumer group, JSON serialization/deserialization, manual acknowledgment, retry handling, DLT, and a DLT consumer.

The following are additional improvements we have discussed but have not yet implemented: worker-thread processing, consumer idempotency, transactional outbox, schema evolution, and comprehensive observability.

Application
     ↓
Database Transaction
     ├── Business Data
     └── Outbox Event
              ↓
       Outbox Publisher
              ↓
        Kafka Producer
              ↓
      Employee Events Topic
         (Partitions)
              ↓
       Kafka Consumer Group
              ↓
       Consumer Processing
              ↓
      Idempotency Check
              ↓
       Business Operation
              ↓
    Acknowledgment / Offset
              ↓
   Retry on Processing Failure
              ↓
             DLT
              ↓
        DLT Consumer

Observability: Logs + Metrics + Traces
Schema Management: Versioning + Compatibility

This diagram represents a target architecture, not the complete current implementation.


## How to explain  project in an interview

I developed a Spring Boot application integrated with Apache Kafka to understand event-driven architecture. I implemented a Kafka producer that publishes employee events as JSON and a consumer using Spring Kafka's @KafkaListener. I configured a topic with two partitions and used a consumer group to manage consumption. I also implemented manual acknowledgment, retry handling with DefaultErrorHandler, a Dead Letter Topic, and a separate DLT consumer to handle failed messages.

As further reliability improvements, I would introduce consumer idempotency to handle duplicate delivery, a Transactional Outbox Pattern to address database-to-Kafka consistency, schema evolution for compatibility, and observability for monitoring failures and consumer lag.
