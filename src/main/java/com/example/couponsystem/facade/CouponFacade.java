package com.example.couponsystem.facade;

import com.example.couponsystem.dto.CouponRequest;
import com.example.couponsystem.kafka.producer.CouponIssueProducer;
import com.example.couponsystem.repository.CouponRedisRepository;
import com.example.couponsystem.repository.LockRepository;
import com.example.couponsystem.repository.LockRepository2;
import com.example.couponsystem.service.AsyncCouponService;
import com.example.couponsystem.service.CouponService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponFacade {

    private final LockRepository lockRepository;
    private final LockRepository2 lockRepository2;
    private final CouponService couponService;
    private final CouponRedisRepository couponRedisRepository;
    private final RedissonClient redissonClient;
    private final CouponIssueProducer producer;
    private final AsyncCouponService asyncCouponService;

    public void issueCouponWithRetry(CouponRequest in) {
        int maxRetry = 10;

        for (int attempt = 1; attempt <= maxRetry; attempt++) {
            try {
                couponService.issueCoupon(in);
                return;
            } catch (OptimisticLockingFailureException e) {
                if (attempt == maxRetry) {
                    break;
                }

                long delayMs = calculateJitterDelay(attempt);

                log.warn("버전 충돌 발생. attempt={}/{}, {}ms 후 재시도", attempt, maxRetry, delayMs);

                sleep(delayMs);
            }
        }

        throw new IllegalStateException("재시도 횟수 초과");
    }

    /**
     * 네임드 락
     */
    public void issueCouponWithNamedLock(CouponRequest in) {
        String lockKey = "coupon_issue_" + in.getCouponId();
        lockRepository2.executeWithLocK(lockKey, 10, () -> {
            couponService.issueCoupon(in);
        });
    }

    /**
     * Exponential backoff + jitter
     * 예:
     * 1회차: 10~30ms
     * 2회차: 20~50ms
     * 3회차: 40~90ms
     * ...
     * 최대 200ms 제한
     */
    private long calculateJitterDelay(int attempt) {
        long baseDelay = Math.min(10L * (1L << Math.min(attempt - 1, 4)), 200L);
        long jitter = ThreadLocalRandom.current().nextLong(0, 21); // 0~20ms
        return baseDelay + jitter;
    }

    private void sleep(long delayMs) {
        LockSupport.parkNanos(delayMs * 1_000_000L);
    }

    /**
     * 레디스
     */
    public void issueCouponWithRedis(CouponRequest in) {
        Long count = couponRedisRepository.increment(in.getCouponId());

        if (count > 1000000) {
            couponRedisRepository.decrement(in.getCouponId());
            throw new IllegalStateException("발급 수량 초과");
        }

        try {
            couponService.issueCoupon(in);
        } catch (Exception e) {
            couponRedisRepository.decrement(in.getCouponId());
            throw e;
        }
    }

    /**
     * Redisson 분산락
     */
    public void issueCouponWithRedissonLock(CouponRequest in) throws Exception {

        Long couponId = in.getCouponId();

        // 1. Redisson 분산락 획득 (키: "lock:coupon:{couponId}")
        RLock lock = redissonClient.getLock("lock:coupon:" + couponId);
        boolean acquired = lock.tryLock(5, TimeUnit.SECONDS);

        if (!acquired) {
            throw new IllegalStateException("락 획득 실패");
        }

        try {
            // 2. Redis INCR + DB 저장
            Long count = couponRedisRepository.increment(in.getCouponId());
            if (count > 1000000) {
                couponRedisRepository.decrement(in.getCouponId());
                throw new IllegalStateException("발급 수량 초과");
            }
        } finally {
            // 3. finally에서 락 해제
            lock.unlock();
        }

        // 락 해제 후 DB 저장 - 락 유지 시간 최소화
        couponService.issueCoupon(in);
    }

    public boolean issueCouponWithKafka(CouponRequest in) throws Exception {
        Long count = couponRedisRepository.increment(in.getCouponId());

        // 제한을 확 줄여서 딱 1000개만 발급되는지 확인해 보자.
        if (count > 1000) {
            couponRedisRepository.decrement(in.getCouponId());
            return false;
        }

        try {
            producer.send(in.getCouponId(), in.getUserId());
        } catch (Exception e) {
            couponRedisRepository.decrement(in.getCouponId());
            throw e;
        }

        return true;
    }

    // 비동기
    public boolean issueCouponWithAsync(CouponRequest in) {
        Long count = couponRedisRepository.increment(in.getCouponId());

        if (count > 1000) {
            couponRedisRepository.decrement(in.getCouponId());
            return false;
        }

        asyncCouponService.issueCouponAsync(in);
        return true;
    }
}
