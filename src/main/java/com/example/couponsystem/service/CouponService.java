package com.example.couponsystem.service;

import com.example.couponsystem.dto.CouponRequest;
import com.example.couponsystem.entity.CouponIssue;
import com.example.couponsystem.kafka.message.CouponIssueMessage;
import com.example.couponsystem.repository.CouponIssueRepository;
import com.example.couponsystem.repository.CouponRepository;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final CouponIssueRepository couponIssueRepository;
    private final JdbcTemplate jdbcTemplate;

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

    @Transactional
    public int[] issueCouponsBatch(List<CouponIssueMessage> messages) {
        if (messages.isEmpty()) {
            return new int[0];
        }

        String sql = "INSERT IGNORE INTO coupon_issue (coupon_id, user_id, issued_at) VALUES (?, ?, ?)";
        LocalDateTime now = LocalDateTime.now();

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws java.sql.SQLException {
                CouponIssueMessage message = messages.get(i);
                ps.setLong(1, message.getCouponId());
                ps.setLong(2, message.getUserId());
                ps.setTimestamp(3, Timestamp.valueOf(now));
            }

            @Override
            public int getBatchSize() {
                return messages.size();
            }
        });
    }
}
