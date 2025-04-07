package com.yoanesber.redis_stream_consumer.util;

import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HelperUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static Map<String, Object> convertToMap(Object entity) throws IllegalArgumentException {
        if (entity == null) {
            throw new IllegalArgumentException("Entity cannot be null");
        }
        
        if (entity instanceof String) {
            throw new IllegalArgumentException("Entity cannot be a String");
        }
        if (entity instanceof Number) {
            throw new IllegalArgumentException("Entity cannot be a Number");
        }
        if (entity instanceof Boolean) {
            throw new IllegalArgumentException("Entity cannot be a Boolean");
        }
        
        try {
            return objectMapper.convertValue(entity, new TypeReference<Map<String, Object>>() {});
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to convert entity to Map", e);
        }
    }
}
