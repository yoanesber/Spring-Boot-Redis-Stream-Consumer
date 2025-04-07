package com.yoanesber.redis_stream_consumer.service.Impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.yoanesber.redis_stream_consumer.service.OrderPaymentService;
import com.yoanesber.redis_stream_consumer.util.HelperUtil;

@Service
public class OrderPaymentServiceImpl implements OrderPaymentService {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Boolean handlePaymentSuccess(Object orderPayment) {
        logger.info("Processing payment success: " + orderPayment);

        // convert order payment to map for easier processing
        Map<String, Object> orderPaymentMap = null;
        try {
            orderPaymentMap = HelperUtil.convertToMap(orderPayment);

            // add logic to handle payment success
            // for example, update order status in database or update retryCount field 
        } catch (IllegalArgumentException e) {
            logger.error("Failed to convert order payment to map: " + e.getMessage(), e);
            return false;
        }
        
        // assuming orderPaymentMap contains invalid payment method
        if (orderPaymentMap.get("paymentMethod").equals("CC")) {
            logger.error("Order payment is not valid: " + orderPaymentMap);
            return false;
        }
        
        return true;
    }

    @Override
    public Boolean handlePaymentFailed(Object orderPayment) {
        logger.info("Processing payment failed: " + orderPayment);

        // add logic to handle payment failure
        // for example, send notification to support team or update order status in database

        return true;
    }
}
