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
public class Coupon {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private int totalQuantity;

    private int issuedCount;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    public boolean availableIssue() {
        return issuedCount < totalQuantity;
    }

    public void issue() {
        if (!availableIssue()) {
            throw new IllegalStateException(
                    "발급 가능한 수량을 초과했습니다. total: %d, issued: %d"
                            .formatted(totalQuantity, issuedCount));
        }
        issuedCount++;
    }
}
