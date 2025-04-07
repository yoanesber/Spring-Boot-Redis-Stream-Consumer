package com.yoanesber.redis_stream_consumer.service;

public interface OrderPaymentService {
    // Handle payment success event
    Boolean handlePaymentSuccess(Object orderPayment);
    
    // Handle payment failed event
    Boolean handlePaymentFailed(Object orderPayment);
}
