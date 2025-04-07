package com.yoanesber.redis_stream_consumer.redis;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.yoanesber.redis_stream_consumer.util.HelperUtil;
import com.yoanesber.redis_stream_consumer.service.OrderPaymentService;

@Component
@SuppressWarnings("unchecked")
public class MessageConsumer {

    private final OrderPaymentService orderPaymentService;
    private final RedisTemplate<String, Object> redisTemplate;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String DLQ_POSTFIX = ".dlq"; // Dead Letter Queue postfix
    private final String RETRY_PREFIX = "retry-"; // Retry prefix

    public MessageConsumer(OrderPaymentService orderPaymentService,
        RedisTemplate<String, Object> redisTemplate) {
        this.orderPaymentService = orderPaymentService;
        this.redisTemplate = redisTemplate;
    }

    /**
     * Creates a consumer group for the specified stream if it doesn't already exist.
     *
     * @param streamName the name of the Redis stream
     * @param groupName  the name of the consumer group
     */
    private void createGroupIfNotExists(String streamName, String groupName) {
        try {
            redisTemplate.opsForStream()
                .createGroup(streamName, ReadOffset.latest(), groupName);

            logger.info("Group created successfully: {}", groupName);
        } catch (Exception e) {
            // Check if the exception is due to the group already existing
            if (e.getCause() != null && e.getCause().getMessage() != null &&
                e.getCause().getMessage().contains("BUSYGROUP Consumer Group name already exists")) {
                logger.info("Group already exists: {}", groupName);
            } else {
                logger.error("Error creating group: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Processes the message based on the stream name.
     *
     * @param streamName   the name of the Redis stream
     * @param messageData  the data of the message to be processed
     * @return true if the message was processed successfully, false otherwise
     */
    private Boolean performHandler(String streamName, Object messageData) {
        // Check if the message data is null or empty
        if (messageData == null) {
            logger.warn("Message data is null for stream: {}", streamName);
            return false; // Invalid message data, return false
        }

        // Decide the action based on the stream name
        // Add your logic here to handle different stream names
        // For example, if the stream name is "PAYMENT_SUCCESS", call the appropriate service method
        try {
            if (streamName.equals("PAYMENT_SUCCESS")) {
                return orderPaymentService.handlePaymentSuccess(messageData);
            } else if (streamName.equals("PAYMENT_FAILED")) {
                return orderPaymentService.handlePaymentFailed(messageData);
            } else {
                logger.warn("Unknown stream name: {}", streamName);
                return false; // Unknown stream name, return false
            }
        } catch (Exception e) {
            logger.error("Error processing message for stream: {}", streamName, e);
            return false; // Error occurred while processing the message, return false
        }
    }

    /**
     * Reads messages from a Redis stream.
     *
     * @param streamName    the name of the Redis stream to read from
     * @param groupName     the name of the consumer group
     * @param consumerName  the name of the consumer
     * @param messagesCount the maximum number of messages to read
     * @param blockDuration the duration in seconds to block if no messages are available
     * @return true if messages were read successfully, false otherwise
     */
    public Boolean readMessages(String streamName, 
        String groupName, String consumerName, 
        long messagesCount, long blockDuration) {
        // Check if the stream name is null or empty
        if (streamName == null || streamName.isEmpty()) {
            logger.error("Stream name is null or empty.");
            return false; // Invalid stream name, return false
        }

        // Check if the group name is null or empty
        if (groupName == null || groupName.isEmpty()) {
            logger.error("Group name is null or empty.");
            return false; // Invalid group name, return false
        }

        // Check if the consumer name is null or empty
        if (consumerName == null || consumerName.isEmpty()) {
            logger.error("Consumer name is null or empty.");
            return false; // Invalid consumer name, return false
        }

        // Check if the messages count is less than or equal to 0
        if (messagesCount <= 0) {
            logger.error("Messages count must be greater than 0.");
            return false; // Invalid messages count, return false
        }

        // Check if the block duration is less than or equal to 0
        if (blockDuration <= 0) {
            logger.error("Block duration must be greater than 0.");
            return false; // Invalid block duration, return false
        }

        // Create a consumer group for the stream if it doesn't exist
        createGroupIfNotExists(streamName, groupName);

        // Initialize an empty list to store messages 
        List<MapRecord<String, Object, Object>> messages = List.of();
        try {
            // Initialize the StreamReadOptions with the desired parameters
            StreamReadOptions options = StreamReadOptions.empty()
                .count(messagesCount)   // Read up to `n` messages
                .block(Duration.ofSeconds(blockDuration)); // Blocks the read operation for a specified timeout (milliseconds), waiting for new messages

            // Set the stream offset to the start of the stream
            // This will read all messages from the beginning of the stream
            StreamOffset<String> streamOffset = StreamOffset.create(streamName, ReadOffset.lastConsumed());
            
            // Read messages from the stream using the Redis template
            messages = redisTemplate.opsForStream().read(
                Consumer.from(groupName, consumerName), 
                options, streamOffset);
        } catch (Exception e) {
            logger.error("Error reading messages from stream: {}", streamName, e);
            return false;
        }

        // Check if messages are null or empty
        if (messages == null || messages.isEmpty()) {
            logger.info("No messages found in the stream for stream: {}, group: {}, consumer: {}", 
                streamName, groupName, consumerName);
            return true; // No messages to process, return true
        }

        // Process each message in the list
        for (MapRecord<String, Object, Object> message : messages) {
            // Get the message ID and data
            String messageId = message.getId().getValue();
            Object messageData = message.getValue();

            // Check if the message data is null or empty
            if (messageData == null) {
                logger.warn("Message data is null for message ID: {}", messageId);
                continue;
            }

            // Process the message based on the stream name
            if (performHandler(streamName, messageData)) {
                // Acknowledge the message if processed successfully
                redisTemplate.opsForStream().acknowledge(streamName, groupName, messageId);
                logger.info("Message with ID {} processed successfully and acknowledged.", messageId);
            } else {
                logger.error("Failed to process message with ID: {}", messageId);

                // Do nothing, the message will be retried later
                // Message will be in the Pending Entries List (PEL), because messages 
                //  in Redis Streams that are read but not acknowledged
            }
        }

        // Return true if all messages were processed successfully
        return true;
    }


    /**
     * Retries pending messages in the Redis stream.
     *
     * @param streamName      the name of the Redis stream
     * @param groupName       the name of the consumer group
     * @param consumerName    the name of the consumer
     * @param messagesCount   the maximum number of messages to retry
     * @param maxIdleTime     the duration in seconds for a message to be considered idle
     * @param maxDeliveryCount the maximum delivery count for a message to be retried
     * @return true if pending messages were retried successfully, false otherwise
     */
    public Boolean retryPendingMessages(String streamName, String groupName, 
        String consumerName, long messagesCount, long maxIdleTime, long maxDeliveryCount) {
        
        // Check if the stream name is null or empty
        if (streamName == null || streamName.isEmpty()) {
            logger.error("Stream name is null or empty.");
            return false; // Invalid stream name, return false
        }

        // Check if the group name is null or empty
        if (groupName == null || groupName.isEmpty()) {
            logger.error("Group name is null or empty.");
            return false; // Invalid group name, return false
        }

        // Check if the consumer name is null or empty
        if (consumerName == null || consumerName.isEmpty()) {
            logger.error("Consumer name is null or empty.");
            return false; // Invalid consumer name, return false
        }

        // Check if the messages count is less than or equal to 0
        if (messagesCount <= 0) {
            logger.error("Messages count must be greater than 0.");
            return false; // Invalid messages count, return false
        }

        // Check if the max idle time is less than or equal to 0
        if (maxIdleTime <= 0) {
            logger.error("Max idle time must be greater than 0.");
            return false; // Invalid max idle time, return false
        }

        // Check if the max delivery count is less than or equal to 0
        if (maxDeliveryCount <= 0) {
            logger.error("Max delivery count must be greater than 0.");
            return false; // Invalid max delivery count, return false
        }

        // Initialize an empty list to store pending messages
        PendingMessages pendingMessages = null;

        // Get the pending messages for the specified stream and consumer group
        // This will return a list of pending messages that have not been acknowledged yet
        try {
            pendingMessages = redisTemplate.opsForStream()
                .pending(streamName, Consumer.from(groupName, consumerName), 
                    Range.unbounded(), messagesCount);

        } catch (Exception e) {
            logger.error("Error retrieving pending messages: {}", e.getMessage(), e);
            return false; // Error occurred while retrieving pending messages, return false
        }

        // Check if pending messages are null or empty
        // If they are, we will try to get the pending messages for the retry consumer
        // To ensure that we can retry messages that have been claimed by the retry consumer
        String retryConsumer = RETRY_PREFIX + consumerName;
        if (pendingMessages == null || pendingMessages.isEmpty()) {
            logger.info("No pending messages found for consumer: {}. Checking retry consumer.", consumerName);

            pendingMessages = redisTemplate.opsForStream()
                .pending(streamName, Consumer.from(groupName, retryConsumer), 
                    Range.unbounded(), messagesCount);
        }

        // Check if pending messages are still null or empty
        if (pendingMessages == null || pendingMessages.isEmpty()) {
            logger.info("No pending messages found in the stream for group: {}, consumer: {}", 
                groupName, retryConsumer);

            return true; // No pending messages to retry, return true
        }

        // Process each pending message in the list
        for (PendingMessage pendingMessage : pendingMessages) {
            // Get the message ID and data
            // The message ID is the unique identifier for the message in the stream
            // idleTime is the time since the message was last delivered to a consumer
            // deliveryCount is the total number of times the message has been delivered to consumers
            RecordId pendingMessageId = pendingMessage.getId();
            long idleTime = pendingMessage.getElapsedTimeSinceLastDelivery().toSeconds();
            long deliveryCount = pendingMessage.getTotalDeliveryCount();

            // Check if the message has exceeded the maximum idle time
            if (idleTime >= maxIdleTime) {
                logger.info("Retrying ({} attempts) message with ID: {} after {} seconds of idle time for consumer: {}",
                    deliveryCount, pendingMessageId, idleTime, consumerName);

                // Initialize an empty list to store messages 
                List<MapRecord<String, Object, Object>> messages = List.of();

                // Attempt to claim the message from the stream
                // This will move the message from the pending list to the retry consumer
                try {
                    messages = redisTemplate.opsForStream()
                        .claim(streamName, groupName, retryConsumer, 
                            Duration.ofSeconds(maxIdleTime), pendingMessageId);
                } catch (Exception e) {
                    logger.error("Error claiming message: {}", e.getMessage(), e);
                    continue; // Skip to the next pending message if an error occurs
                }

                // Check if messages are null or empty
                if (messages == null || messages.isEmpty()) {
                    logger.info("No messages found in the stream for ID: {}", pendingMessageId);  
                    continue; // No messages to process, skip to the next pending message
                }

                // Process message with index 0
                MapRecord<String, Object, Object> message = messages.get(0);

                // Get the message ID and data
                String messageId = message.getId().getValue();
                Object messageData = message.getValue();
    
                // Check if the message data is null or empty
                if (messageData == null) {
                    logger.warn("Message data is null for message ID: {}", messageId);
                    continue; // Skip to the next pending message if data is null
                }
    
                // Process the message based on the stream name
                if (performHandler(streamName, messageData)) {
                    // Acknowledge the message if processed successfully
                    redisTemplate.opsForStream().acknowledge(streamName, groupName, messageId);
                    logger.info("Message with ID {} processed in retry successfully and acknowledged.", messageId);
                } else {
                    // Handle the case where the message processing failed
                    logger.error("Failed to process message with ID: {}", messageId);

                    // Check if the message has exceeded the maximum delivery count
                    if (deliveryCount >= maxDeliveryCount) {
                        logger.error("Maximum delivery count exceeded for message ID: {}", messageId);
                        
                        // Move the message to the Dead Letter Queue (DLQ)
                        if (moveToDLQ(streamName, groupName, consumerName, messageId, messageData)) {
                            logger.info("Message with ID {} moved to DLQ.", messageId);
                        } else {
                            logger.error("Failed to move message with ID {} to DLQ.", messageId);
                        }
                    } else {
                        // Do nothing, just continue to the next pending message
                        continue; // Skip to the next pending message
                    }
                }
            }
        }

        // Return true if all pending messages were processed successfully
        return true;
    }

    /**
     * Moves a message to the Dead Letter Queue (DLQ).
     * A Dead Letter Queue is a separate stream (or queue) 
     *  where you put messages that failed processing after a certain number of retries
     *  so they don’t keep clogging your main stream.
     *
     * @param streamName   the name of the Redis stream
     * @param groupName    the name of the consumer group
     * @param consumerName the name of the consumer
     * @param messageId    the ID of the message to be moved
     * @param messageData  the data of the message to be moved
     * @return true if the message was moved successfully, false otherwise
     */
    private Boolean moveToDLQ(String streamName, String groupName, 
        String consumerName, String messageId, Object messageData) {
            
        // Check if the stream name is null or empty
        if (streamName == null || streamName.isEmpty()) {
            logger.error("Stream name is null or empty.");
            return false;
        }

        // Check if the group name is null or empty
        if (groupName == null || groupName.isEmpty()) {
            logger.error("Group name is null or empty.");
            return false;
        }

        // Check if the consumer name is null or empty
        if (consumerName == null || consumerName.isEmpty()) {
            logger.error("Consumer name is null or empty.");
            return false;
        }

        // Check if the message ID is null or empty
        if (messageId == null || messageId.isEmpty()) {
            logger.error("Message ID is null or empty.");
            return false;
        }

        // Check if the message data is null or empty
        if (messageData == null) {
            logger.error("Message data is null.");
            return false;
        }

        // Creating a map from the message object
        Map<String, Object> messageMap = HelperUtil.convertToMap(messageData);
        if (messageMap == null) {
            logger.error("Failed to convert message to map: {}", messageData);
            throw new RuntimeException("Failed to convert message to map: " + messageData);
        }

        // Move the message to the Dead Letter Queue (DLQ)
        try {
            // Add the message to the DLQ stream
            // The DLQ stream name is derived from the original stream name by appending ".dlq"
            String dlqStreamName = streamName + DLQ_POSTFIX;
            redisTemplate.opsForStream().add(
                StreamRecords.newRecord()
                    .withId(messageId)
                    .in(dlqStreamName)
                    .ofMap(messageMap));
        } catch (Exception e) {
            logger.error("Error moving message to DLQ: {}", e.getMessage(), e);
            return false;
        }

        // Acknowledge the message, indicating that it has been moved to the DLQ
        // This will remove the message from the pending list of the original stream
        redisTemplate.opsForStream().acknowledge(streamName, groupName, messageId);

        return true;
    }
}