package com.yoanesber.redis_stream_consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Value("${spring.data.redis.host}")
    private String redisHost;

    @Value("${spring.data.redis.port}")
    private int redisPort;

    @Value("${spring.data.redis.username}")
    private String redisUsername;

    @Value("${spring.data.redis.password}")
    private String redisPassword;

    @Value("${spring.data.redis.timeout}")
    private long redisTimeout;

    @Value("${spring.data.redis.lettuce.shutdown-timeout}")
    private long shutdownTimeout;

    @Value("${spring.data.redis.connect-timeout}")
    private long connectTimeout;

    /*
     * Create a shared instance of ClientResources to be used by LettuceConnectionFactory.
     * ClientResources is a shared resource that manages the lifecycle of the Lettuce client.
     */

    @Bean(destroyMethod = "shutdown")
    public ClientResources clientResources() {
        return DefaultClientResources.create();
    }

    /*
     * Create a LettuceConnectionFactory bean that uses the shared ClientResources.
     * LettuceConnectionFactory is a RedisConnectionFactory implementation for Lettuce.
     * It is used to create a Redis connection.
     * LettuceConnectionFactory is used by RedisTemplate to create a Redis connection.
     * RedisTemplate is used by RedisPublisher to publish messages to Redis.
     * 
     * LettuceConnectionFactory requires two configurations:
     * 1. RedisStandaloneConfiguration: defines the Redis server configuration
     * 2. LettuceClientConfiguration: defines the Lettuce client configuration
     * 
     * In RedisStandaloneConfiguration we set the following properties:
     * * hostName: defines the Redis server host name
     * * port: defines the Redis server port
     * * username: defines the Redis server username
     * * password: defines the Redis server password
     * 
     * In LettuceClientConfiguration we set the following properties:
     * * commandTimeout: defines the maximum amount of time to wait for a command to complete before timing out; If the command is not completed within this time, a TimeoutException is thrown
     * * clientResources: defines the shared client resources; It is used to manage the lifecycle of the Lettuce client
     * * clientOptions: defines the Lettuce client options
     * * shutdownTimeout: defines the maximum amount of time to wait for the client to close gracefully; If the client is not closed within this time, a TimeoutException is thrown
     * * socketOptions: defines the socket options for the client
     * * connectTimeout: defines the maximum amount of time to wait for a connection to be established before timing out; If the connection is not established within this time, a TimeoutException is thrown
     */
    @Bean
    public LettuceConnectionFactory redisConnectionFactory(ClientResources clientResources) {
        // Configure RedisStandaloneConfiguration
        RedisStandaloneConfiguration serverConfig = new RedisStandaloneConfiguration();
        serverConfig.setHostName(redisHost);
        serverConfig.setPort(redisPort);
        serverConfig.setUsername(redisUsername);
        serverConfig.setPassword(redisPassword);

        // Configure LettuceClientConfiguration
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
            .commandTimeout(Duration.ofSeconds(redisTimeout)) // Set timeout for commands; default is 60s
            .shutdownTimeout(Duration.ofSeconds(shutdownTimeout)) // Graceful shutdown; default is 100ms
            .clientResources(clientResources) // Use shared client resources
            .clientOptions(ClientOptions.builder()
                .socketOptions(SocketOptions.builder()
                    .connectTimeout(Duration.ofSeconds(connectTimeout)) // Set connection timeout; default is 10s
                    .build())
                .build())
            .build();

        return new LettuceConnectionFactory(serverConfig, clientConfig);
    }

     /*
     * Create a RedisTemplate bean that uses the LettuceConnectionFactory.
     * RedisTemplate is a high-level abstraction for interacting with Redis.
     * It provides methods for executing Redis commands and operations.
     * RedisTemplate is used by RedisPublisher to publish messages to Redis.
     * 
     * RedisTemplate requires two configurations:
     * 1. KeySerializer: defines the serializer for keys
     * 2. ValueSerializer: defines the serializer for values
     * 
     * In RedisTemplate we set the following serializers:
     * * StringRedisSerializer: serializer for keys
     * * GenericJackson2JsonRedisSerializer: serializer for values
     * 
     * StringRedisSerializer is used to serialize keys as strings.
     * GenericJackson2JsonRedisSerializer is used to serialize values as JSON.
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory(this.clientResources()));

        // Create ObjectMapper with JavaTimeModule
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule()); // Enables Java 8 Time support
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // Optional: Store dates as ISO-8601

        // Use GenericJackson2JsonRedisSerializer with custom ObjectMapper
        GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer(objectMapper);

        // Use String serializer for keys (channels)
        redisTemplate.setKeySerializer(new StringRedisSerializer());

        // Use JSON serializer for values (messages)
        redisTemplate.setValueSerializer(serializer);

        // Use String serializer for hash keys (channels)
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());

        // Use JSON serializer for hash values (messages)
        redisTemplate.setHashValueSerializer(serializer);

        // Initialize RedisTemplate after setting serializers
        redisTemplate.afterPropertiesSet();

        return redisTemplate;
    }
}