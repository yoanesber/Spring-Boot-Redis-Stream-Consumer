package com.yoanesber.redis_stream_consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RedisStreamConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(RedisStreamConsumerApplication.class, args);
	}

}
