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
