package com.example.couponsystem.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CouponIssue {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long couponId;

    private Long userId;

    private LocalDateTime issuedAt;

    public static CouponIssue create(Long couponId, Long userId) {
        CouponIssue issue = new CouponIssue();
        issue.couponId = couponId;
        issue.userId = userId;
        issue.issuedAt = LocalDateTime.now();
        return issue;
    }
}
