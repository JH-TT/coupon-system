# Phase 1: Naive 구현 — Race Condition 체감

## 목표

동시성 제어 없이 쿠폰 발급 API를 구현하고, 부하 테스트로 Race Condition을 직접 체감한다.

---

## 구현 내용

### 핵심 로직 — `CouponService.issueCoupon()`

```java
@Transactional
public void issueCoupon(CouponRequest in) {
    Coupon coupon = couponRepository.findById(couponId).orElseThrow(...);

    // 중복 발급 체크
    Optional<CouponIssue> issue = couponIssueRepository.findByCouponIdAndUserId(couponId, userId);
    if (issue.isPresent()) {
        throw new IllegalArgumentException("이미 발급받은 쿠폰입니다.");
    }

    // 수량 체크 + 발급
    coupon.issue();                          // issuedCount++ (메모리에서)
    couponIssueRepository.save(newIssue);    // coupon_issue INSERT
}
```

### 수량 체크 — `Coupon.issue()`

```java
public void issue() {
    if (!availableIssue()) {  // issuedCount < totalQuantity
        throw new IllegalStateException("발급 가능한 수량을 초과했습니다.");
    }
    issuedCount++;
}
```

**문제 지점**: `findById()` → `issue()` → JPA dirty checking으로 UPDATE 사이에 **아무런 동시성 제어가 없다.**

---

## 테스트 환경

| 항목 | 설정 |
|------|------|
| 쿠폰 수량 | 100장 (total_quantity = 100) |
| 동시 사용자 | 200명 (k6 VU) |
| 요청 방식 | 각 유저가 1번씩 동시 발급 요청 |
| DB | MySQL 8 (Docker) |
| 앱 | Spring Boot 3.5.0, JPA (ddl-auto: validate) |

### k6 스크립트 핵심 설정

```javascript
export const options = {
  scenarios: {
    spike: {
      executor: 'shared-iterations',
      vus: 200,
      iterations: 200,
      maxDuration: '30s',
    },
  },
};
```

---

## 테스트 결과

### k6 출력

```
✗ status is 200
  ↳  36% — ✓ 73 / ✗ 127

http_req_duration..............: avg=519.15ms  p(95)=661.45ms
http_req_failed................: 63.50%  127 out of 200
success_count..................: 73
fail_count.....................: 127
```

### DB 조회 결과

```sql
SELECT COUNT(*) FROM coupon_issue;
-- 결과: 73

SELECT issued_count FROM coupon WHERE id = 1;
-- 결과: 30
```

| 지표 | 기대값 | 실제값 | 판정 |
|------|--------|--------|------|
| coupon_issue 행 수 | - | **73건** | - |
| coupon.issued_count | **73** | **30** | **불일치** |
| 유실된 증가분 | 0 | **43건** | Lost Update |

### Spring Boot 콘솔 에러

```
Deadlock found when trying to get lock; try restarting transaction
```

---

## 원인 분석

### 문제 1: Lost Update

여러 스레드가 **같은 시점의 issued_count를 읽고**, 각자 +1 한 뒤 덮어쓴다.

```
Thread A: SELECT issued_count → 5
Thread B: SELECT issued_count → 5      ← 같은 값
Thread C: SELECT issued_count → 5      ← 같은 값
Thread A: UPDATE issued_count = 6
Thread B: UPDATE issued_count = 6      ← A의 증가분 소실
Thread C: UPDATE issued_count = 6      ← A, B의 증가분 소실
```

3건이 처리됐지만 issued_count는 **1만 증가**. 이 패턴이 반복되면서 43건이 유실됐다.

### 문제 2: Deadlock

중복 체크 SELECT가 결과 없음으로 인해 **Gap Lock**을 획득한 뒤,
INSERT가 같은 갭에 **Insert Intention Lock**을 요청하면서 **교차 대기**가 발생한다.

```
Thread A: SELECT coupon_issue WHERE coupon_id=1 AND user_id=3
          → 행 없음 → Gap Lock 획득

Thread B: SELECT coupon_issue WHERE coupon_id=1 AND user_id=7
          → 행 없음 → Gap Lock 획득 (같은 갭)

Thread A: INSERT coupon_issue → Insert Intention Lock 요청
          → Thread B의 Gap Lock에 차단

Thread B: INSERT coupon_issue → Insert Intention Lock 요청
          → Thread A의 Gap Lock에 차단

→ 순환 대기 = Deadlock → MySQL이 한쪽 트랜잭션을 강제 롤백
```

> **참고**: `coupon_issue` 테이블에 `(coupon_id, user_id)` unique index가 없으므로,
> 중복 체크 SELECT는 non-unique 스캔으로 Next-Key Lock(Gap Lock 포함)을 획득한다.
> 행이 존재하지 않으면 어떤 경우든 Gap Lock이 잡힌다.

127건의 실패 중 상당수가 이 Deadlock으로 인한 것이다.

### 문제 3: 초과 발급 위험

issued_count가 실제보다 낮게 유지되기 때문에 `availableIssue()` 체크를 계속 통과한다.
VU를 500~1000으로 늘리면 **100장 한정 쿠폰이 200장 이상 발급**될 수 있다.

---

## 결론

| 문제 | 증상 | 근본 원인 |
|------|------|-----------|
| Lost Update | issued_count 불일치 (30 vs 73) | 동시 READ → 각자 +1 → 덮어쓰기 |
| Deadlock | 트랜잭션 강제 롤백 (127건 실패) | INSERT gap lock + UPDATE 교차 대기 |
| 초과 발급 | 트래픽 증가 시 100장 초과 가능 | issued_count가 실제보다 낮아 체크 통과 |

**동시성 제어 없는 naive 구현은 실 서비스에서 절대 사용할 수 없다.**

→ Phase 2에서 DB 락 전략(비관적/낙관적/네임드)을 적용하여 해결한다.
