package com.example.couponsystem.kafka.producer;

import com.example.couponsystem.kafka.message.CouponIssueMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponIssueProducer {

    private static final String TOPIC = "coupon-issue";
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(Long couponId, Long userId) throws JsonProcessingException {
        CouponIssueMessage message = new CouponIssueMessage(couponId, userId);
        String payload = objectMapper.writeValueAsString(message);

        try {
            kafkaTemplate.send(TOPIC, String.valueOf(couponId), payload).get(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka 이벤트 발행 중 인터럽트 발생", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Kafka 이벤트 발행 실패", e);
        }
    }
}
