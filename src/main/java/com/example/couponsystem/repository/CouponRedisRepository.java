package com.example.couponsystem.repository;

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
        String key = "coupon:" + couponId + ":count";
        Long value = redisTemplate.opsForValue().decrement(key);

        System.out.println("key = " + key + ", value = " + value);
    }
}
