package com.example.couponsystem.service;

import com.example.couponsystem.dto.CouponRequest;
import com.example.couponsystem.entity.Coupon;
import com.example.couponsystem.entity.CouponIssue;
import com.example.couponsystem.repository.CouponIssueRepository;
import com.example.couponsystem.repository.CouponRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final CouponIssueRepository couponIssueRepository;

    @Transactional
    public void issueCoupon(CouponRequest in) {

        Long couponId = in.getCouponId();
        Long userId = in.getUserId();

        // 쿠폰 존재여부 확인
        couponRepository.findById(couponId).orElseThrow(
                () -> new IllegalArgumentException("Coupon coupon with id " + couponId + " not found!")
        );

        CouponIssue newIssue = CouponIssue.create(couponId, userId);

        // 쿠폰 발급정보 저장.
        couponIssueRepository.save(newIssue);
    }
}
