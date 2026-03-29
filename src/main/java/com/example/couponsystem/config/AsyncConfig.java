package com.example.couponsystem.config;

import java.util.concurrent.Executor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean("couponExecutor")
    public Executor couponExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(50);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(10000); // 큐가 버퍼 역할 (Kafka 토픽과 대응)
        executor.setThreadNamePrefix("coupon-async-");
        executor.initialize();
        return executor;
    }
}
