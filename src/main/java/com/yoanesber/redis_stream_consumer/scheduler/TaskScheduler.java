package com.yoanesber.redis_stream_consumer.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.yoanesber.redis_stream_consumer.redis.MessageConsumer;

/**
 * It uses a thread pool task scheduler to run tasks asynchronously.
 * The class is responsible for reading messages from Redis streams and processing them using the MessageConsumer.
 */
@Component
@SuppressWarnings("unused")
public class TaskScheduler {

    private final String PAYMENT_SUCCESS_STREAM = "PAYMENT_SUCCESS";
    private final String PAYMENT_FAILED_STREAM = "PAYMENT_FAILED";
    private final String ORDER_PAYMENT_GROUP = "ORDER_PAYMENT";
    private final String PAYMENT_SUCCESS_CONSUMER = "payment-success-consumer-1";
    private final String PAYMENT_FAILED_CONSUMER = "payment-failed-consumer-1";
    private final String RETRY_PREFIX = "retry-";

    private final long DEFAULT_MESSAGES_COUNT = 3L; // number of messages to read
    private final long DEFAULT_BLOCK_DURATION = 5L; // in seconds
    private final long DEFAULT_RETRY_MESSAGE_COUNT = 3L; // number of messages to retry    
    private final long DEFAULT_RETRY_MAX_IDLE_DURATION = 60L; // in seconds
    private final long DEFAULT_RETRY_MAX_DELIVERY_COUNT = 3L; // max delivery count

    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private final MessageConsumer messageConsumer;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public TaskScheduler(@Qualifier("threadPoolTaskScheduler") ThreadPoolTaskScheduler threadPoolTaskScheduler,
        MessageConsumer messageConsumer) {
        this.threadPoolTaskScheduler = threadPoolTaskScheduler;
        this.messageConsumer = messageConsumer;
    }

    /**
     * This method is scheduled to run every 5 seconds. 
     * It reads messages from the "PAYMENT_SUCCESS" stream and processes them.
     */
    @Async("threadPoolTaskScheduler")
    @Scheduled(cron = "*/5 * * * * ?") // every 5 seconds
    public void handlePaymentSuccess() {
        logger.info("Cron task runs on thread " + Thread.currentThread().getName() + " for handlePaymentSuccess");

        try {
            if (!messageConsumer.readMessages(PAYMENT_SUCCESS_STREAM, 
                ORDER_PAYMENT_GROUP, PAYMENT_SUCCESS_CONSUMER, 
                DEFAULT_MESSAGES_COUNT, DEFAULT_BLOCK_DURATION)) {
                logger.error("Failed to read messages from stream: " + PAYMENT_SUCCESS_STREAM);
                return;
            }
            
            logger.info("Cron task on thread " + Thread.currentThread().getName() + " for handlePaymentSuccess has been completed");
        } catch (Exception e) {
            logger.error("Exception on thread " + Thread.currentThread().getName() + " for handlePaymentSuccess with message: " + e.getMessage());
        }
    }

    /**
     * This method is scheduled to run every 5 seconds. 
     * It reads messages from the "PAYMENT_FAILED" stream and processes them.
     */
    @Async("threadPoolTaskScheduler")
    @Scheduled(cron = "*/5 * * * * ?") // every 5 seconds
    public void handlePaymentFailed() {
        logger.info("Cron task runs on thread " + Thread.currentThread().getName() + " for handlePaymentFailed");

        try {
            if (!messageConsumer.readMessages(PAYMENT_FAILED_STREAM, 
                ORDER_PAYMENT_GROUP, PAYMENT_FAILED_CONSUMER, 
                DEFAULT_MESSAGES_COUNT, DEFAULT_BLOCK_DURATION)) {
                logger.error("Failed to read messages from stream: " + PAYMENT_FAILED_STREAM);
                return;
            }
            
            logger.info("Cron task on thread " + Thread.currentThread().getName() + " for handlePaymentFailed has been completed");
        } catch (Exception e) {
            logger.error("Exception on thread " + Thread.currentThread().getName() + " for handlePaymentFailed with message: " + e.getMessage());
        }
    }

    /**
     * This method is scheduled to run every 5 seconds. 
     * It retries messages from the "PAYMENT_SUCCESS" stream that have not been processed successfully.
     */
    @Async("threadPoolTaskScheduler")
    @Scheduled(cron = "*/5 * * * * ?") // every 5 seconds
    public void retryPaymentSuccess() {
        logger.info("Cron task runs on thread " + Thread.currentThread().getName() + " for retryPaymentSuccess");

        try {
            // Retry messages from the PAYMENT_SUCCESS stream
            if (!messageConsumer.retryPendingMessages(PAYMENT_SUCCESS_STREAM, 
                ORDER_PAYMENT_GROUP, PAYMENT_SUCCESS_CONSUMER, DEFAULT_RETRY_MESSAGE_COUNT,
                DEFAULT_RETRY_MAX_IDLE_DURATION, DEFAULT_RETRY_MAX_DELIVERY_COUNT)) {
                logger.error("Failed to retry messages from stream: " + PAYMENT_SUCCESS_STREAM);
                return;
            }
            
            logger.info("Cron task on thread " + Thread.currentThread().getName() + " for retryPaymentSuccess has been completed");
        } catch (Exception e) {
            logger.error("Exception on thread " + Thread.currentThread().getName() + " for retryPaymentSuccess with message: " + e.getMessage());
        }
    }

    /**
     * This method is scheduled to run every 5 seconds. 
     * It retries messages from the "PAYMENT_FAILED" stream that have not been processed successfully.
     */
    @Async("threadPoolTaskScheduler")
    @Scheduled(cron = "*/5 * * * * ?") // every 5 seconds
    public void retryPaymentFailed() {
        logger.info("Cron task runs on thread " + Thread.currentThread().getName() + " for retryPaymentFailed");

        try {
            // Retry messages from the PAYMENT_FAILED stream    
            if (!messageConsumer.retryPendingMessages(PAYMENT_FAILED_STREAM, 
                ORDER_PAYMENT_GROUP, PAYMENT_FAILED_CONSUMER, DEFAULT_RETRY_MESSAGE_COUNT,
                DEFAULT_RETRY_MAX_IDLE_DURATION, DEFAULT_RETRY_MAX_DELIVERY_COUNT)) {
                logger.error("Failed to retry messages from stream: " + PAYMENT_FAILED_STREAM);
                return;
            }
            
            logger.info("Cron task on thread " + Thread.currentThread().getName() + " for retryPaymentFailed has been completed");
        } catch (Exception e) {
            logger.error("Exception on thread " + Thread.currentThread().getName() + " for retryPaymentFailed with message: " + e.getMessage());
        }
    }
}