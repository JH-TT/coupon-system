package com.example.couponsystem.service;

import com.example.couponsystem.dto.CouponRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AsyncCouponService {

    private final CouponService couponService;

    @Async("couponExecutor")
    public void issueCouponAsync(CouponRequest in) {
        couponService.issueCoupon(in);
    }
}
