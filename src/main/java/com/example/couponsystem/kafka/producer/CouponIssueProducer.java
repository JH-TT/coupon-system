package com.example.couponsystem.kafka.producer;

import com.example.couponsystem.kafka.message.CouponIssueMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    public void send(Long couponId, Long userId) {
        String payload;

        try {
            CouponIssueMessage message = new CouponIssueMessage(couponId, userId);
            payload = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Kafka payload 직렬화 실패", e);
        }

        kafkaTemplate.send(TOPIC, String.valueOf(userId), payload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Kafka 이벤트 발행 실패. couponId={}, userId={}", couponId, userId, ex);
                        return;
                    }

                    if (result != null && result.getRecordMetadata() != null) {
                        log.debug(
                                "Kafka 이벤트 발행 성공. topic={}, partition={}, offset={}, couponId={}, userId={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                couponId,
                                userId
                        );
                    }
                });
    }
}
