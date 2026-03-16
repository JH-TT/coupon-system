# Phase 2: DB 락 전략 비교 — 비관적/낙관적/네임드 락

## 목표

Phase 1에서 발견한 Lost Update + Deadlock을 DB 락으로 해결한다.
3가지 전략을 동일 조건으로 테스트하고 비교한다.

---

## 테스트 환경

| 항목 | 설정 |
|------|------|
| 쿠폰 수량 | 100장 (total_quantity = 100) |
| 동시 사용자 | 200명 (k6 VU) |
| 요청 방식 | 각 유저가 1번씩 동시 발급 요청 |
| DB | MySQL 8 (Docker) |
| 앱 | Spring Boot 3.5.0, JPA (ddl-auto: validate) |

---

## 전략 1: 비관적 락 (Pessimistic Lock)

### 구현

`SELECT ... FOR UPDATE`로 조회 시점에 행을 잠근다.

```java
// CouponRepository
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT c FROM Coupon c WHERE c.id = :id")
Optional<Coupon> findByIdWithPessimisticLock(@Param("id") Long id);
```

```java
// CouponService — findById를 findByIdWithPessimisticLock으로 변경
Coupon coupon = couponRepository.findByIdWithPessimisticLock(couponId).orElseThrow(...);
```

### 동작 원리

```
Thread A: SELECT coupon FOR UPDATE → 행 잠금 획득
Thread B: SELECT coupon FOR UPDATE → Thread A 끝날 때까지 대기
Thread A: INSERT coupon_issue + UPDATE coupon + COMMIT → 잠금 해제
Thread B: 최신 issued_count를 읽고 진행
```

### 테스트 결과

```
✗ status is 200
  ↳  50% — ✓ 100 / ✗ 100

http_req_duration..............: avg=2.28s     p(95)=5.57s
  { expected_response:true }...: avg=4.34s
http_req_failed................: 50.00%  100 out of 200
success_count..................: 100
```

```sql
SELECT COUNT(*) FROM coupon_issue;     -- 결과: 100
SELECT issued_count FROM coupon WHERE id = 1;  -- 결과: 100
```

| 지표 | 결과 |
|------|------|
| coupon_issue | **100건** |
| issued_count | **100** ✅ |
| 데이터 일치 | **일치** ✅ |
| Deadlock | **없음** ✅ |

---

## 전략 2: 낙관적 락 (Optimistic Lock)

### 구현

`@Version` 컬럼으로 UPDATE 시 버전 충돌을 감지한다. 충돌 시 재시도.

```java
// Coupon 엔티티
@Version
private Long version;
```

```java
// CouponFacade — 프록시 경유를 위해 별도 클래스로 분리
public void issueCouponWithRetry(CouponRequest in) {
    int maxRetry = 10;
    for (int attempt = 1; attempt <= maxRetry; attempt++) {
        try {
            couponService.issueCoupon(in);  // 프록시 경유 → @Transactional 정상 작동
            return;
        } catch (OptimisticLockingFailureException e) {
            // 재시도
        }
    }
    throw new IllegalStateException("재시도 횟수 초과");
}
```

### 구현 시 주의: Spring 프록시 자기 호출 문제

`issueCouponWithRetry()`를 `CouponService` 안에 두면 `this.issueCoupon()` 호출이 되어
**Spring AOP 프록시를 우회**한다. `@Transactional`이 무시되어 INSERT만 커밋되고 UPDATE는 반영되지 않는다.

반드시 **별도 클래스(CouponFacade)**로 분리하여 프록시를 경유해야 한다.

```
Controller → CouponFacade.issueCouponWithRetry()
                  ↓ (프록시 경유)
             CouponService.issueCoupon()  ← @Transactional 정상 작동
```

### 동작 원리

```sql
-- JPA가 자동으로 version 체크를 추가
UPDATE coupon SET issued_count=?, version=version+1 WHERE id=? AND version=?
-- version이 불일치하면 OptimisticLockException → 재시도
```

### 테스트 결과

```
✗ status is 200
  ↳  3% — ✓ 7 / ✗ 193

http_req_duration..............: avg=2.31s     p(95)=2.83s
  { expected_response:true }...: avg=2.61s
http_req_failed................: 96.50%  193 out of 200
success_count..................: 7
```

```sql
SELECT COUNT(*) FROM coupon_issue;     -- 결과: 7
SELECT issued_count FROM coupon WHERE id = 1;  -- 결과: 7
```

| 지표 | 결과 |
|------|------|
| coupon_issue | **7건** |
| issued_count | **7** ✅ |
| 데이터 일치 | **일치** ✅ |
| Deadlock | **발생** ❌ |

### 왜 7장밖에 발급 못 했는가

200명이 동시에 같은 version을 읽고, 1명만 UPDATE 성공, 나머지 전원 충돌 → 재시도.
재시도 시에도 같은 상황 반복 → maxRetry(10) 소진 → 탈락.

**핫 리소스(모두가 같은 행을 수정) + 높은 동시성**에서 낙관적 락은 재시도 폭풍이 발생한다.

### 왜 Deadlock이 발생하는가

`@Version`은 DB 락이 아니다. SELECT 시점에 아무 락도 안 잡으므로 여러 스레드가 동시에 진행한다.

```
Thread A: SELECT coupon (락 없음) → 진행
Thread B: SELECT coupon (락 없음) → 동시 진행

Thread A: SELECT coupon_issue WHERE coupon_id=1 AND user_id=3 (중복 체크)
          → 행 없음 → Gap Lock 획득
Thread B: SELECT coupon_issue WHERE coupon_id=1 AND user_id=7 (중복 체크)
          → 행 없음 → Gap Lock 획득 (같은 갭)

Thread A: INSERT coupon_issue → Insert Intention Lock 요청
          → Thread B의 Gap Lock에 차단
Thread B: INSERT coupon_issue → Insert Intention Lock 요청
          → Thread A의 Gap Lock에 차단
→ 순환 대기 = Deadlock
```

Phase 1(No Lock)과 동일한 원인. `@Version`은 UPDATE 시 충돌만 감지할 뿐,
중복 체크 SELECT의 Gap Lock과 INSERT의 Insert Intention Lock 충돌은 막지 못한다.

---

## 전략 3: 네임드 락 (Named Lock)

### 구현

MySQL의 `GET_LOCK()` — 테이블/행 락이 아닌 문자열 이름 기반의 유저 레벨 락.

```java
// LockRepository — DataSource 직접 사용 (같은 커넥션 보장)
@Repository
@RequiredArgsConstructor
public class LockRepository {

    private final DataSource dataSource;

    public void executeWithLock(String key, int timeout, Runnable action) {
        try (Connection conn = dataSource.getConnection()) {
            try {
                getLock(conn, key, timeout);
                action.run();
            } finally {
                releaseLock(conn, key);
            }
        } catch (SQLException e) {
            throw new RuntimeException("락 처리 중 오류", e);
        }
    }
}
```

```java
// CouponFacade
public void issueCouponWithNamedLock(CouponRequest in) {
    String lockKey = "coupon_issue_" + in.getCouponId();
    lockRepository.executeWithLock(lockKey, 10, () -> {
        couponService.issueCoupon(in);
    });
}
```

### 구현 시 겪은 문제들

**1. `ResultSet` 리턴 타입 + SQL 오타**

```java
// 잘못된 코드
@Query(value = "SELET RELEASE_LOCK(:key)", nativeQuery = true)  // SELET 오타
ResultSet releaseLock(@Param("key") String key);  // ResultSet 매핑 불가
// → Statement.executeQuery() cannot issue statements that do not produce result sets.
```

`ResultSet` → `Integer`, `SELET` → `SELECT`로 수정.

**2. 커넥션 분리 문제**

JpaRepository 사용 시 `GET_LOCK`과 `RELEASE_LOCK`이 **서로 다른 커넥션**에서 실행될 수 있다.
네임드 락은 커넥션에 종속되므로, 다른 커넥션에서 `RELEASE_LOCK` 호출 시 `NULL` 반환 → NPE.

→ `DataSource`에서 직접 커넥션을 꺼내 **같은 커넥션으로 lock/release** 하도록 변경.

**3. 커넥션 풀 데드락**

`DataSource.getConnection()`으로 락용 커넥션을 잡은 상태에서,
내부 `@Transactional`이 비즈니스용 커넥션을 추가로 요청 → 요청당 커넥션 2개 필요.

```
HikariCP pool = 10 (기본값)
Thread 1~10: 각각 lock용 커넥션 획득 → pool 소진
Thread 1:    issueCoupon() → @Transactional 커넥션 필요 → pool 비었음 → 타임아웃
→ 전원 타임아웃
```

→ `maximum-pool-size: 250`, `connection-timeout: 10000`으로 조정.

### 테스트 결과

```
✗ status is 200
  ↳  50% — ✓ 100 / ✗ 100

http_req_duration..............: avg=10.49s    p(95)=10.97s
  { expected_response:true }...: avg=10.16s
http_req_failed................: 50.00%  100 out of 200
success_count..................: 100
```

```sql
SELECT COUNT(*) FROM coupon_issue;     -- 결과: 100
SELECT issued_count FROM coupon WHERE id = 1;  -- 결과: 100
```

| 지표 | 결과 |
|------|------|
| coupon_issue | **100건** |
| issued_count | **100** ✅ |
| 데이터 일치 | **일치** ✅ |
| Deadlock | **없음** ✅ |

---

## 전체 비교

| 지표 | No Lock | 비관적 락 | 낙관적 락 | 네임드 락 |
|------|---------|-----------|-----------|-----------|
| 성공 | 73/200 | **100/200** | 7/200 | **100/200** |
| issued_count | 30 ❌ | 100 ✅ | 7 ✅ | 100 ✅ |
| coupon_issue | 73 ❌ | 100 ✅ | 7 ✅ | 100 ✅ |
| 데이터 일치 | **불일치** | **일치** | **일치** | **일치** |
| 쿠폰 100장 소진 | 미달 | ✅ | 미달 | ✅ |
| 평균 응답 (성공) | 519ms | **4.34s** | 2.61s | **10.16s** |
| Deadlock | 발생 | 없음 | 발생 | 없음 |
| 구현 난이도 | - | **낮음** | 중간 | **높음** |
| 커넥션 사용 | 1개 | 1개 | 1개 | **2개** |

---

## Deadlock 심화: 락 순서가 핵심이다

### 비관적 락에서 Deadlock이 안 나는 이유

```
항상: SELECT coupon FOR UPDATE (잠금) → INSERT coupon_issue → UPDATE coupon
→ 진입점에서 직렬화되므로 순환 대기가 불가능
```

### 비관적 락에서도 Deadlock을 발생시키는 방법

순서를 뒤집으면 된다: 중복 체크 + INSERT를 먼저, FOR UPDATE를 나중에.

```java
// Deadlock 발생 코드 (테스트 후 원복)
Optional<CouponIssue> issue = couponIssueRepository.findByCouponIdAndUserId(couponId, userId);
// ↑ 행 없음 → Gap Lock 획득
couponIssueRepository.saveAndFlush(newIssue);   // Insert Intention Lock → 다른 Gap Lock과 충돌 가능
Coupon coupon = couponRepository.findByIdWithPessimisticLock(couponId);  // FOR UPDATE
coupon.issue();
```

```
Thread A: SELECT coupon_issue (중복 체크) → 행 없음 → Gap Lock 획득
Thread B: SELECT coupon_issue (중복 체크) → 행 없음 → Gap Lock 획득 (같은 갭)
Thread A: INSERT coupon_issue → Insert Intention Lock 요청 → Thread B의 Gap Lock에 차단
Thread B: INSERT coupon_issue → Insert Intention Lock 요청 → Thread A의 Gap Lock에 차단
→ 순환 대기 = Deadlock
```

### 결론

> **"비관적 락 = Deadlock 위험"이 아니다.**
> **"락 획득 순서가 일관되지 않으면 Deadlock이 발생한다"가 정확한 표현이다.**

---

## 전략별 적합한 상황

| 전략 | 적합한 상황 | 부적합한 상황 |
|------|------------|--------------|
| **비관적 락** | 동시 쓰기가 많은 핫 리소스 (쿠폰, 재고) | 읽기 위주의 낮은 충돌 환경 |
| **낙관적 락** | 충돌이 드문 일반적인 수정 작업 | 핫 리소스 + 높은 동시성 |
| **네임드 락** | 분산 환경에서 테이블 락 없이 동기화 | 단순 단일 행 보호 (오버 엔지니어링) |

## 이 시나리오(선착순 쿠폰) 결론

> **비관적 락이 압도적 승자.** 단순하고, 정확하고, 커넥션 효율적.

→ Phase 3에서 Redis 원자적 연산 + 분산락으로 DB 의존도를 낮추고 성능을 개선한다.
