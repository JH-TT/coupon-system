package com.example.couponsystem.kafka.consumer;

import com.example.couponsystem.kafka.message.CouponIssueMessage;
import com.example.couponsystem.repository.CouponRedisRepository;
import com.example.couponsystem.service.CouponService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponIssueConsumer {

    private final CouponService couponService;
    private final CouponRedisRepository couponRedisRepository;
    private final ObjectMapper mapper;

    @KafkaListener(topics = "coupon-issue", groupId = "coupon-issue-group")
    public void consume(List<String> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        List<CouponIssueMessage> issueMessages = new ArrayList<>(messages.size());
        for (String message : messages) {
            try {
                issueMessages.add(parseMessage(message));
            } catch (Exception e) {
                log.error("Kafka 메시지 파싱 실패. message={}", message, e);
            }
        }

        if (issueMessages.isEmpty()) {
            return;
        }

        try {
            int[] batchResult = couponService.issueCouponsBatch(issueMessages);
            Map<Long, Long> duplicateCountsByCoupon = new HashMap<>();

            for (int i = 0; i < batchResult.length; i++) {
                int result = batchResult[i];

                if (result > 0 || result == Statement.SUCCESS_NO_INFO) {
                    continue;
                }

                if (result == Statement.EXECUTE_FAILED) {
                    CouponIssueMessage failedMessage = issueMessages.get(i);
                    compensate(failedMessage, new IllegalStateException("Batch execute failed for coupon issue message"));
                    continue;
                }

                CouponIssueMessage issueMessage = issueMessages.get(i);
                duplicateCountsByCoupon.merge(issueMessage.getCouponId(), 1L, Long::sum);
            }

            for (Map.Entry<Long, Long> entry : duplicateCountsByCoupon.entrySet()) {
                couponRedisRepository.decrementBy(entry.getKey(), entry.getValue());
                log.debug("중복 발급 이벤트 보정. couponId={}, duplicateCount={}", entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            for (CouponIssueMessage issueMessage : issueMessages) {
                compensate(issueMessage, e);
            }
        }
    }

    private CouponIssueMessage parseMessage(String message) {
        try {
            CouponIssueMessage issueMessage = mapper.readValue(message, CouponIssueMessage.class);

            if (issueMessage.getCouponId() == null || issueMessage.getUserId() == null) {
                throw new IllegalArgumentException("couponId, userId는 필수입니다.");
            }

            return issueMessage;
        } catch (Exception e) {
            throw new IllegalArgumentException("Kafka 메시지 파싱 실패", e);
        }
    }

    private void compensate(CouponIssueMessage issueMessage, Exception e) {
        boolean firstCompensation = couponRedisRepository.markCompensationRequested(
                issueMessage.getCouponId(),
                issueMessage.getUserId()
        );

        if (firstCompensation) {
            couponRedisRepository.decrement(issueMessage.getCouponId());
            log.error("소비 실패 보상 완료. couponId={}, userId={}", issueMessage.getCouponId(), issueMessage.getUserId(), e);
            return;
        }

        log.error("소비 실패 보상 생략(이미 처리됨). couponId={}, userId={}", issueMessage.getCouponId(), issueMessage.getUserId(), e);
    }

}
