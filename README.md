# Spring Boot Redis Stream Consumer with ThreadPoolTaskScheduler Integration

## 📖 Overview  

This Spring Boot application serves as a **resilient Redis Stream consumer** for processing order payment events (`PAYMENT_SUCCESS` and `PAYMENT_FAILED`). It leverages `ThreadPoolTaskScheduler` to schedule stream polling at fixed intervals, ensuring reliable and scalable event-driven processing.  

Unlike traditional Pub/Sub models, this implementation uses **Redis Streams with Consumer Groups** to guarantee message durability and exactly-once processing semantics. Messages are retained in Redis until they are explicitly acknowledged. Redis interprets the **acknowledgment** as: this message was correctly processed so it can be evicted from the consumer group.  


### 💡 Why Redis Streams?  

Unlike Pub/Sub, Redis Streams offer:  

- **Persistence** – Messages are stored in Redis until explicitly acknowledged by a consumer.
- **Reliability** – Ensures that no messages are lost — perfect for critical systems like payments.
- **Scalability** – Built-in support for consumer groups and horizontal scaling.
- **Replayability** – Failed or pending messages can be retried, replayed, or analyzed.


### 💡 What is a Consumer Group in Redis Streams?  

A **Consumer Group** in Redis Streams is a mechanism for distributing and managing data consumption by **multiple consumers** in a parallel and coordinated manner. While `XREAD` (regular) is suitable for a single consumer, a Consumer Group (`XREADGROUP`) is ideal for multiple consumers processing the stream together. A Consumer Group allows multiple consumers to share the workload of processing messages without duplication. Each message is delivered to only one consumer in the group.  


#### 💡 Why Do We Need Consumer Groups?  

- To enable multiple consumers to collaborate in processing messages.
- To track which messages have been read and which are still pending.
- To retry processing if a message fails.
- To ensure each message is read by only one consumer, unlike Pub/Sub where all consumers receive the same message.


#### 💡 How Consumer Groups Work?

1. A stream is created (`XADD`).  
2. A consumer group is created (`XGROUP CREATE`).  
3. Multiple consumers join the group and start processing messages (`XREADGROUP`).  
4. Messages are assigned to a consumer within the group (only one consumer gets each message).  
5. Consumers acknowledge (`XACK`) processed messages, so Redis knows they are handled.  

### 📌 Redis Stream Message ID (RecordId)  

Each message published to a Redis stream is assigned a unique `RecordId` which acts as its primary identifier in the stream. This ID is composed of a **timestamp and sequence number**, ensuring uniqueness even for multiple messages added in the same millisecond.  


### 💡 What is PEL?  

The **Pending Entries List (PEL)** in Redis Streams acts as a queue of "messages being processed" for each consumer group. Redis tracks the following details for each message in the PEL:  

- **Message ID**: The unique identifier of the message.  
- **Consumer**: The consumer currently processing the message.  
- **Delivery Count**: The number of times the message has been delivered.  
- **Last Delivery Timestamp**: The timestamp of the last delivery attempt.  

**Note:** If a consumer reads a message but does not acknowledge it (`XACK`), the message remains in the PEL. This ensures that unprocessed or failed messages can be retried or reassigned to another consumer.  


### 💡 What is Dead Letter Queue (DLQ)?  

A **Dead Letter Queue** is a separate stream (or queue) where you put **messages that failed processing** after a certain number of retries — so they don’t keep clogging your main stream.  

This allows you to:  
- Avoid infinite retry loops.
- Investigate or alert on problematic messages.
- Optionally reprocess them later (manually or scheduled).


### 🧩 Architecture Overview

This project implements a reliable event-driven architecture using Redis Streams to handle Order Payment creation and processing. Below is a breakdown of the full flow:  

```text
[Client]──▶ (HTTP POST /order-payment)──▶ [Spring Boot API] ──▶ [Redis Stream: PAYMENT_SUCCESS or PAYMENT_FAILED]
                                                                                         │
                                                                                         ▼
                                                                            [StreamConsumer - every 5s]
                                                                                         │
                                                                        ┌────────────────┬──────────────────────┐
                                                                        ▼                ▼                      ▼
                                                                [✔ Processed]   [🔁Retry Queue]  [❌ DLQ (failed after max attempts)]
```

#### 🔄 How It Works  

1. Client Request  
A client sends an HTTP POST request to the `/order-payment` endpoint with the necessary order payment data.  
2. Spring Boot API (Producer)  
- The API receives the request, processes the initial business logic, and then publishes a message to the appropriate Redis stream:  
    - `PAYMENT_SUCCESS` if the payment is successful.  
    - `PAYMENT_FAILED` if the payment fails validation or processing.  
- Each message sent to the stream includes a manually generated `RecordId`, ensuring consistent tracking and ordering.  
3. Redis Streams  
- Redis Streams persist these messages until they are acknowledged by a consumer.  
- This allows for reliable message delivery, replay, and tracking of pending/unprocessed messages.  
4. StreamConsumer (Scheduled Every 5 Seconds)  
- A scheduled consumer job runs every 5 seconds, using `XREADGROUP` to read new or pending messages from the stream as part of a consumer group.  
- It attempts to process each message accordingly:
    - **✅Processed Successfully**: The consumer handles the message and sends an `XACK` to acknowledge its completion. The message is then removed from the pending list.
    - **🔁Retry Queue**: If processing fails temporarily, the message is **not acknowledged**, allowing it to be retried in the next cycle. If its idle time exceeds a threshold, the consumer can reclaim the message for retry using `XCLAIM`.
    - **❌Dead Letter Queue (DLQ)**: If the message fails after exceeding the maximum delivery attempts, it is moved to a DLQ stream for manual inspection, alerting, or later analysis.


### 🚀 Features  

Below are the core features that make this solution robust and ready for real-world scenarios:  

- **StreamConsumer** processes messages in real-time every 5 seconds using scheduler
- Acknowledgment (`XACK`) after successful processing
- Retry mechanism for unacknowledged or failed messages
- **Dead Letter Queue (DLQ)** support for messages that exceed retry limits


### ⏱ Scheduled Stream Consumers  

The application defines the following scheduled tasks, each executed at a fixed rate:  

- **handlePaymentSuccess** – Reads and processes new messages from the PAYMENT_SUCCESS stream.  
- **handlePaymentFailed** – Reads and processes new messages from the PAYMENT_FAILED stream.  
- **retryPaymentSuccess** – Handles unacknowledged messages from the PAYMENT_SUCCESS stream by inspecting the Pending Entries List (PEL) and reprocessing eligible entries.  
- **retryPaymentFailed** – Performs retry logic for messages in the PAYMENT_FAILED stream that remain unacknowledged.  


### 📥 Message Consumer Responsibilities  

1. **Read Messages** – Uses the `XREADGROUP` command to consume messages from Redis Streams under a specific consumer group. The message is then routed for processing based on its originating stream (success or failure).  
2. **retryPendingMessages** – Processes entries from the **PEL** using `XPENDING` and `XCLAIM`:
- Detects messages that have exceeded the maximum idle time.
- Reclaims the message to the current consumer (retry consumer) for retry.
- Tracks the delivery count of each message.
- If a message exceeds the maximum delivery attempts, it is routed to a **Dead Letter Queue (DLQ)** to prevent infinite retries and allow for manual intervention or alerting.

---

## 🤖 Tech Stack  

The technology used in this project are:  

- `Spring Boot Starter Web` – Building RESTful APIs or web applications. Used in this project to handle HTTP requests for creating and managing order payments.
- `Spring Data Redis (Lettuce)` – A high-performance Redis client built on Netty. Integrates Redis seamlessly into Spring, allowing the application to produce and consume Redis Streams with ease.
- `RedisTemplate` – A powerful abstraction provided by Spring Data Redis for performing Redis operations, including stream publishing (XADD), consuming (XREADGROUP), acknowledging (XACK), and more.
- `ThreadPoolTaskScheduler` – Manages scheduled tasks with multi-threading support
- `Lombok` – Reducing boilerplate code
---

## 🏗️ Project Structure  

The project is organized into the following package structure:  

```bash
redis-stream-consumer/
│── src/main/java/com/yoanesber/redis_stream_consumer/
│   ├── 📂config/       # Contains configuration classes, including Redis connection settings.
│   ├── 📂redis/        # Manages Redis stream message consumption, including reading messages from `PAYMENT_SUCCESS` and `PAYMENT_FAILED` streams using consumer groups.
│   ├── 📂scheduler/    # Defines scheduled tasks using `ThreadPoolTaskScheduler` to trigger stream reading and retry logic at a fixed rate.
│   ├── 📂service/      # Encapsulates business logic required to process and act on the consumed payment messages, such as persisting the data, updating order statuses, or notifying external systems.
│   │   ├── 📂impl/     # Implementation of services
│   ├── 📂util/         # Provides helper utilities to support common operations such as message transformation.
```
---

## ⚙ Environment Configuration

Configuration values are stored in `.env.development` and referenced in `application.properties`.  
Example `.env.development` file content:  

```properties
# Application properties
APP_PORT=8081
SPRING_PROFILES_ACTIVE=development

# Redis properties
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_USERNAME=default
REDIS_PASSWORD=your_password
REDIS_TIMEOUT=5
REDIS_CONNECT_TIMEOUT=3
REDIS_LETTUCE_SHUTDOWN_TIMEOUT=10

# Task Scheduler properties
TASK_SCHEDULER_POOL_SIZE=5
TASK_SCHEDULER_THREAD_NAME_PREFIX=thread-
TASK_SCHEDULER_REMOVE_ON_CANCEL_POLICY=false
TASK_SCHEDULER_WAIT_FOR_TASKS_TO_COMPLETE_ON_SHUTDOWN=true
TASK_SCHEDULER_AWAIT_TERMINATION_SECONDS=10
TASK_SCHEDULER_THREAD_PRIORITY=5
TASK_SCHEDULER_CONTINUE_EXISTING_PERIODIC_TASKS_AFTER_SHUTDOWN_POLICY=false
TASK_SCHED_SCHEDULER_EXECUTE_EXISTING_DELAYED_TASKS_AFTER_SHUTDOWN_POLICY=true
TASK_SCHEDULER_DAEMON=false
```

Example `application.properties` file content:  
```properties
# Application properties
spring.application.name=redis-stream-consumer
server.port=${APP_PORT}
spring.profiles.active=${SPRING_PROFILES_ACTIVE}

# Redis properties
spring.data.redis.host=${REDIS_HOST}
spring.data.redis.port=${REDIS_PORT}
spring.data.redis.username=${REDIS_USERNAME}
spring.data.redis.password=${REDIS_PASSWORD}
spring.data.redis.timeout=${REDIS_TIMEOUT}
spring.data.redis.connect-timeout=${REDIS_CONNECT_TIMEOUT}
spring.data.redis.lettuce.shutdown-timeout=${REDIS_LETTUCE_SHUTDOWN_TIMEOUT}

# Task Scheduler properties
spring.task.scheduling.pool.size=${TASK_SCHEDULER_POOL_SIZE}
spring.task.scheduling.thread-name-prefix=${TASK_SCHEDULER_THREAD_NAME_PREFIX}
spring.task.scheduling.remove-on-cancel-policy=${TASK_SCHEDULER_REMOVE_ON_CANCEL_POLICY}
spring.task.scheduling.wait-for-tasks-to-complete-on-shutdown=${TASK_SCHEDULER_WAIT_FOR_TASKS_TO_COMPLETE_ON_SHUTDOWN}
spring.task.scheduling.await-termination-seconds=${TASK_SCHEDULER_AWAIT_TERMINATION_SECONDS}
spring.task.scheduling.thread-priority=${TASK_SCHEDULER_THREAD_PRIORITY}
spring.task.scheduling.continue-existing-periodic-tasks-after-shutdown-policy=${TASK_SCHEDULER_CONTINUE_EXISTING_PERIODIC_TASKS_AFTER_SHUTDOWN_POLICY}
spring.task.scheduling.execute-existing-delayed-tasks-after-shutdown-policy=${TASK_SCHED_SCHEDULER_EXECUTE_EXISTING_DELAYED_TASKS_AFTER_SHUTDOWN_POLICY}
spring.task.scheduling.daemon=${TASK_SCHEDULER_DAEMON}
```
---

## 🛠️ Installation & Setup  

A step by step series of examples that tell you how to get a development env running.  

1. Clone the repository  

```bash
git clone https://github.com/yoanesber/Spring-Boot-Redis-Stream-Consumer.git
cd Spring-Boot-Redis-Stream-Consumer
```

2. Ensure Redis is installed and running:  

```bash
redis-server
```

3. (Optional) If you want to add a specific user with access to a specific channel, you can run the following command in Redis CLI:  

```bash
ACL SETUSER your_user +CHANNEL~your_channel on >your_password
```

4. Set up Redis user and password in `.env.development` file:  

```properties
# Redis properties
REDIS_PASSWORD=your_password
```

5. Build and run the application  

```bash
mvn spring-boot:run
```

---

## 🧪 Testing Scenarios

Below are the descriptions and outcomes of each test scenario along with visual evidence (captured screenshots) to demonstrate the flow and results.  

1. Verify Successful Stream Message Consumption  

To confirm that a valid message is consumed, processed, and acknowledged correctly.  

- Given a new message in the PAYMENT_SUCCESS stream  

```bash
XADD PAYMENT_SUCCESS 1744045049273-0 "id" "\"1744045049273-0\"" "orderId" "\"ORD123456781\"" "amount" "199.99" "currency" "\"USD\"" "paymentMethod" "\"CREDIT_CARD\"" "paymentStatus" "\"SUCCESS\"" "cardNumber" "\"1234 5678 9012 3456\"" "cardExpiry" "\"31/12\"" "cardCvv" "\"123\"" "paypalEmail" "" "bankAccount" "" "bankName" "" "transactionId" "\"TXN1743950189267\"" "retryCount" "0" "createdAt" "\"2025-04-07T14:36:29.268562700Z\"" "updatedAt" "\"2025-04-07T14:36:29.268562700Z\""
```

**Note:** To generate the current timestamp in milliseconds (useful for constructing custom RecordIds manually), you can run the following command in a UNIX-based terminal (Linux/macOS):  

```bash
date +%s%3N
```

This outputs a millisecond-precision timestamp like: `1744044544714`  

- When the handlePaymentSuccess scheduler runs  
```bash
2025-04-07T23:59:25.350+07:00  INFO 21880 --- [redis-stream-consumer] [       thread-4] c.y.r.s.Impl.OrderPaymentServiceImpl     : Processing payment success: {id=1744045049273-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CREDIT_CARD, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
2025-04-07T23:59:25.585+07:00  INFO 21880 --- [redis-stream-consumer] [       thread-4] c.y.r.redis.MessageConsumer              : Message with ID 1744045049273-0 processed successfully and acknowledged.
```

- Then the message should be read, processed successfully:  

```bash
127.0.0.1:6379> XREVRANGE PAYMENT_SUCCESS + - COUNT 1
1) 1) "1744045049273-0"
   2)  1) "id"
       2) "\"1744045049273-0\""
       3) "orderId"
       4) "\"ORD123456781\""
       5) "amount"
       6) "199.99"
       7) "currency"
       8) "\"USD\""
       9) "paymentMethod"
      10) "\"CREDIT_CARD\""
      11) "paymentStatus"
      12) "\"SUCCESS\""z
      13) "cardNumber"
      14) "\"1234 5678 9012 3456\""
      15) "cardExpiry"
      16) "\"31/12\""
      17) "cardCvv"
      18) "\"123\""
      19) "paypalEmail"
      20) ""
      21) "bankAccount"
      22) ""
      23) "bankName"
      24) ""
      25) "transactionId"
      26) "\"TXN1743950189267\""
      27) "retryCount"
      28) "0"
      29) "createdAt"
      30) "\"2025-04-07T14:36:29.268562700Z\""
      31) "updatedAt"
      32) "\"2025-04-07T14:36:29.268562700Z\""
127.0.0.1:6379>
127.0.0.1:6379> XREVRANGE PAYMENT_SUCCESS.dlq + - COUNT 1
(empty array)
127.0.0.1:6379>
```

**Conclusion:** The `PAYMENT_SUCCESS` stream message was handled properly, acknowledged successfully, and not moved into the DLQ — indicating that the consumer logic and stream acknowledgment mechanism are functioning as intended.

2. Simulate Failed Message Processing  

To validate that messages that fail during processing are left unacknowledged and stay in the pending list for retry.  

- Given a message in the PAYMENT_FAILED stream that fails processing  

To simulate a failure scenario, the `OrderPaymentService` logic was intentionally modified to reject payments with an invalid payment method:

```java
if (orderPaymentMap.get("paymentMethod").equals("CC")) {
    logger.error("Order payment is not valid: " + orderPaymentMap);
    return false;
}
```

In this setup, any message containing "paymentMethod": "CC" will be treated as invalid. 
```bash
XADD PAYMENT_SUCCESS 1744046314461-0 "id" "\"1744046314461-0\"" "orderId" "\"ORD123456781\"" "amount" "199.99" "currency" "\"USD\"" "paymentMethod" "\"CC\"" "paymentStatus" "\"SUCCESS\"" "cardNumber" "\"1234 5678 9012 3456\"" "cardExpiry" "\"31/12\"" "cardCvv" "\"123\"" "paypalEmail" "" "bankAccount" "" "bankName" "" "transactionId" "\"TXN1743950189267\"" "retryCount" "0" "createdAt" "\"2025-04-07T14:36:29.268562700Z\"" "updatedAt" "\"2025-04-07T14:36:29.268562700Z\""
```

- When the handlePaymentFailed scheduler runs  

When such a message is published to the Redis stream (e.g., PAYMENT_SUCCESS), the consumer detects the invalid value during processing and logs an error:  

```bash
2025-04-08T00:18:53.503+07:00  INFO 33024 --- [redis-stream-consumer] [       thread-5] c.y.r.s.Impl.OrderPaymentServiceImpl     : Processing payment success: {id=1744046314461-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CC, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
2025-04-08T00:18:53.583+07:00 ERROR 33024 --- [redis-stream-consumer] [       thread-5] c.y.r.s.Impl.OrderPaymentServiceImpl     : Order payment is not valid: {id=1744046314461-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CC, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
```

- Then the message should remain unacknowledged and enter the Pending Entries List (PEL)  


3. Test Retry Mechanism for Pending Messages and Move The Messages to a DLQ stream

To test the ability to reprocess unacknowledged messages after a certain idle time, then the message should be moved to a DLQ stream such as PAYMENT_SUCCESS.dlq  

```bash
2025-04-08T00:19:55.024+07:00  INFO 33024 --- [redis-stream-consumer] [       thread-2] c.y.r.redis.MessageConsumer              : Retrying (1 attempts) message with ID: 1744046314461-0 after 61 seconds of idle time for consumer: payment-success-consumer-1
2025-04-08T00:19:55.042+07:00  INFO 33024 --- [redis-stream-consumer] [       thread-2] c.y.r.s.Impl.OrderPaymentServiceImpl     : Processing payment success: {id=1744046314461-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CC, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
2025-04-08T00:19:55.046+07:00 ERROR 33024 --- [redis-stream-consumer] [       thread-2] c.y.r.s.Impl.OrderPaymentServiceImpl     : Order payment is not valid: {id=1744046314461-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CC, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
2025-04-08T00:19:55.049+07:00 ERROR 33024 --- [redis-stream-consumer] [       thread-2] c.y.r.redis.MessageConsumer              : Failed to process message with ID: 1744046314461-0
2025-04-08T00:20:55.021+07:00  INFO 33024 --- [redis-stream-consumer] [       thread-1] c.y.r.redis.MessageConsumer              : Retrying (2 attempts) message with ID: 1744046314461-0 after 60 seconds of idle time for consumer: payment-success-consumer-1
2025-04-08T00:20:55.028+07:00  INFO 33024 --- [redis-stream-consumer] [       thread-1] c.y.r.s.Impl.OrderPaymentServiceImpl     : Processing payment success: {id=1744046314461-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CC, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
2025-04-08T00:20:55.030+07:00 ERROR 33024 --- [redis-stream-consumer] [       thread-1] c.y.r.s.Impl.OrderPaymentServiceImpl     : Order payment is not valid: {id=1744046314461-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CC, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
2025-04-08T00:20:55.030+07:00 ERROR 33024 --- [redis-stream-consumer] [       thread-1] c.y.r.redis.MessageConsumer              : Failed to process message with ID: 1744046314461-0
2025-04-08T00:21:55.002+07:00  INFO 33024 --- [redis-stream-consumer] [       thread-5] c.y.r.redis.MessageConsumer              : Retrying (3 attempts) message with ID: 1744046314461-0 after 60 seconds of idle time for consumer: payment-success-consumer-1
2025-04-08T00:21:55.010+07:00  INFO 33024 --- [redis-stream-consumer] [       thread-5] c.y.r.s.Impl.OrderPaymentServiceImpl     : Processing payment success: {id=1744046314461-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CC, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
2025-04-08T00:21:55.011+07:00 ERROR 33024 --- [redis-stream-consumer] [       thread-5] c.y.r.s.Impl.OrderPaymentServiceImpl     : Order payment is not valid: {id=1744046314461-0, orderId=ORD123456781, amount=199.99, currency=USD, paymentMethod=CC, paymentStatus=SUCCESS, cardNumber=1234 5678 9012 3456, cardExpiry=31/12, cardCvv=123, paypalEmail=null, bankAccount=null, bankName=null, transactionId=TXN1743950189267, retryCount=0, createdAt=2025-04-07T14:36:29.268562700Z, updatedAt=2025-04-07T14:36:29.268562700Z}
2025-04-08T00:21:55.011+07:00 ERROR 33024 --- [redis-stream-consumer] [       thread-5] c.y.r.redis.MessageConsumer              : Failed to process message with ID: 1744046314461-0
2025-04-08T00:21:55.011+07:00 ERROR 33024 --- [redis-stream-consumer] [       thread-5] c.y.r.redis.MessageConsumer              : Maximum delivery count exceeded for message ID: 1744046314461-0
2025-04-08T00:21:55.025+07:00  INFO 33024 --- [redis-stream-consumer] [       thread-5] c.y.r.redis.MessageConsumer              : Message with ID 1744046314461-0 moved to DLQ.
```



**Conclusion:** During testing, a message with `RecordID 1744046314461-0` was intentionally sent with an invalid `paymentMethod = CC` to simulate a failure scenario. The consumer retried processing this message three times, with each attempt occurring after a 60-second idle period. On each retry, the `OrderPaymentService` detected the invalid payment and logged an error. After reaching the configured maximum retry threshold (3 attempts), the message was not acknowledged and was successfully moved to the **Dead Letter Queue (DLQ)** stream (`PAYMENT_SUCCESS.dlq`). This behavior demonstrates the system’s robustness in handling persistent failures by isolating problematic messages without losing them.  

You can verify the message in the DLQ using the following command:  

```bash
127.0.0.1:6379> XREVRANGE PAYMENT_SUCCESS.dlq + - COUNT 1
1) 1) "1744046314461-0"
   2)  1) "id"
       2) "\"1744046314461-0\""
       3) "orderId"
       4) "\"ORD123456781\""
       5) "amount"
       6) "199.99"
       7) "currency"
       8) "\"USD\""
       9) "paymentMethod"
      10) "\"CC\""
      11) "paymentStatus"
      12) "\"SUCCESS\""
      13) "cardNumber"
      14) "\"1234 5678 9012 3456\""
      15) "cardExpiry"
      16) "\"31/12\""
      17) "cardCvv"
      18) "\"123\""
      19) "paypalEmail"
      20) ""
      21) "bankAccount"
      22) ""
      23) "bankName"
      24) ""
      25) "transactionId"
      26) "\"TXN1743950189267\""
      27) "retryCount"
      28) "0"
      29) "createdAt"
      30) "\"2025-04-07T14:36:29.268562700Z\""
      31) "updatedAt"
      32) "\"2025-04-07T14:36:29.268562700Z\""
127.0.0.1:6379>
```

This confirms the message was safely moved to the DLQ for further inspection or manual handling.  

---


## 🔗 Related Repositories  

- For the Redis Stream as Message Producer implementation, check out [Order Payment Service with Redis Streams as Reliable Message Producer for PAYMENT_SUCCESS / PAYMENT_FAILED Events](https://github.com/yoanesber/Spring-Boot-Redis-Stream-Producer).  
- For the Redis Publisher implementation, check out [Spring Boot Redis Publisher with Lettuce](https://github.com/yoanesber/Spring-Boot-Redis-Publisher-Lettuce).  
- For the Redis Subscriber implementation, check out [Spring Boot Redis Subscriber with Lettuce](https://github.com/yoanesber/Spring-Boot-Redis-Subscriber-Lettuce).  