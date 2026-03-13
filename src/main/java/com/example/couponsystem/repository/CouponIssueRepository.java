package com.example.couponsystem.repository;

import com.example.couponsystem.entity.CouponIssue;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CouponIssueRepository extends JpaRepository<CouponIssue, Long> {

    @Query("select i from CouponIssue i where i.couponId = :couponId and i.userId = :userId")
    Optional<CouponIssue> findByCouponIdAndUserId(@Param("couponId") Long couponId, @Param("userId") Long userId);
}
