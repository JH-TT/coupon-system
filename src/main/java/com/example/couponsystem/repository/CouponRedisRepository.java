package com.example.couponsystem.repository;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class CouponRedisRepository {

    private final RedisTemplate<String, String> redisTemplate;

    public Long increment(Long couponId) {

        String key = "coupon:" + couponId + ":count";
        Long value = redisTemplate.opsForValue().increment(key);

        return value;
    }

    public void decrement(Long couponId) {
        decrementBy(couponId, 1L);
    }

    public void decrementBy(Long couponId, long amount) {
        if (amount <= 0) {
            return;
        }

        String key = "coupon:" + couponId + ":count";
        redisTemplate.opsForValue().decrement(key, amount);
    }

    public boolean markCompensationRequested(Long couponId, Long userId) {
        String key = "coupon:" + couponId + ":compensated:user:" + userId;
        Boolean marked = redisTemplate.opsForValue().setIfAbsent(key, "1", Duration.ofHours(24));
        return Boolean.TRUE.equals(marked);
    }
}
