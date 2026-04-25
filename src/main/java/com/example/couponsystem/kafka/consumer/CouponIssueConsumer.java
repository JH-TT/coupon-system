package com.example.couponsystem.kafka.consumer;

import com.example.couponsystem.dto.CouponRequest;
import com.example.couponsystem.kafka.message.CouponIssueMessage;
import com.example.couponsystem.repository.CouponRedisRepository;
import com.example.couponsystem.service.CouponService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
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
    public void consume(String message) {
        CouponIssueMessage issueMessage = parseMessage(message);

        CouponRequest request = new CouponRequest();
        request.setCouponId(issueMessage.getCouponId());
        request.setUserId(issueMessage.getUserId());

        try {
            couponService.issueCoupon(request);
        } catch (DataIntegrityViolationException e) {
            if (isDuplicateIssue(e)) {
                couponRedisRepository.decrement(issueMessage.getCouponId());
                log.info("중복 발급 이벤트 무시. couponId={}, userId={}", issueMessage.getCouponId(), issueMessage.getUserId());
                return;
            }

            compensate(issueMessage, e);
        } catch (Exception e) {
            compensate(issueMessage, e);
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

    private boolean isDuplicateIssue(DataIntegrityViolationException e) {
        Throwable current = e;

        while (current != null) {
            String message = current.getMessage();

            if (message != null && message.contains("uk_coupon_user")) {
                return true;
            }

            current = current.getCause();
        }

        return false;
    }
}
