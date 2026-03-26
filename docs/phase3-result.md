# Phase 3: Redis 원자적 연산 — INCR 기반 수량 제어

## 목표

Phase 2의 DB 락 방식에서 벗어나, Redis 원자적 연산(INCR)으로 수량을 제어한다.
DB 락 없이 정합성과 성능을 동시에 확보할 수 있는지 검증한다.

---

## 테스트 환경

| 항목 | 설정 |
|------|------|
| 쿠폰 수량 | 100장 |
| 동시 사용자 | 200명 (k6 VU) |
| 요청 방식 | 각 유저가 1번씩 동시 발급 요청 |
| DB | MySQL 8 (Docker) |
| Redis | Redis 7 (Docker) |
| 앱 | Spring Boot 3.5.0 + spring-boot-starter-data-redis |

---

## 설계: Redis를 수량의 단일 진실 원천으로

### A안 vs B안

| 방식 | 수량 체크 | issued_count 관리 | 문제점 |
|------|----------|-------------------|--------|
| A안 | Redis INCR | DB에서도 coupon.issue() | 이중 관리, DB 동시성 문제 잔존 |
| **B안 (채택)** | **Redis INCR** | **사용 안 함** | Redis가 유일한 수량 원천 |

B안을 채택한 이유: Redis INCR로 수량을 제어하면서 DB에서도 issued_count를 관리하면, Redis를 쓰는 의미가 없다. Redis가 "몇 개 발급했는지"의 유일한 진실 원천이 되어야 한다.

---

## 구현

### Redis Repository

```java
@Repository
@RequiredArgsConstructor
public class CouponRedisRepository {

    private final RedisTemplate<String, String> redisTemplate;

    public Long increment(Long couponId) {
        String key = "coupon:" + couponId + ":count";
        return redisTemplate.opsForValue().increment(key);
    }

    public void decrement(Long couponId) {
        String key = "coupon:" + couponId + ":count";
        redisTemplate.opsForValue().decrement(key);
    }
}
```

### Facade (Redis 수량 제어 + DB 저장)

```java
public void issueCouponWithRedis(CouponRequest in) {
    Long count = couponRedisRepository.increment(in.getCouponId());

    if (count > 100) {
        couponRedisRepository.decrement(in.getCouponId());
        throw new IllegalStateException("발급 수량 초과");
    }

    couponService.issueCoupon(in);  // DB에는 coupon_issue만 저장
}
```

### Service (수량 체크/업데이트 제거)

```java
@Transactional
public void issueCoupon(CouponRequest in) {
    Coupon coupon = couponRepository.findById(couponId).orElseThrow(...);

    // 중복 체크만
    Optional<CouponIssue> issue = couponIssueRepository.findByCouponIdAndUserId(couponId, userId);
    if (issue.isPresent()) {
        throw new IllegalArgumentException("이미 발급됨");
    }

    // coupon.issue() 호출 안 함 — Redis가 수량 관리
    couponIssueRepository.save(CouponIssue.create(couponId, userId));
}
```

### 흐름

```
요청 → Redis INCR (원자적)
         ├─ count ≤ 100 → DB INSERT (coupon_issue만)
         └─ count > 100 → Redis DECR + 거부
```

---

## 시행착오 1: 외래키(FK)가 만드는 S Lock → Deadlock

### 증상

외래키가 있는 상태에서 테스트하면 Deadlock이 발생했다.

```
✗ status is 200
  ↳  10% — ✓ 21 / ✗ 179

success_count: 21
coupon:1:count: 100
coupon_issue: 21
```

100개가 Redis를 통과했지만, DB에서 79개가 Deadlock으로 롤백.

### 원인

MySQL에서 자식 테이블에 INSERT할 때 외래키가 있으면 **부모 행에 S Lock(공유 락)**을 건다.

```
Thread A: INSERT coupon_issue → FK 체크 → coupon 행에 S Lock
Thread B: INSERT coupon_issue → FK 체크 → coupon 행에 S Lock
Thread A: (다른 작업에서) coupon 행에 X Lock 필요 → B의 S Lock에 차단
Thread B: (다른 작업에서) coupon 행에 X Lock 필요 → A의 S Lock에 차단
→ 순환 대기 = Deadlock
```

### 해결

외래키 제거. 참조 무결성은 애플리케이션 레벨에서 관리.

> **실무에서도 대규모 트래픽 시스템은 FK를 제거하는 경우가 많다.** FK가 만드는 암묵적 락이 동시성 병목이 되기 때문.

### 추가 발견: Redis-DB 불일치 문제

DB Deadlock으로 트랜잭션이 롤백되어도 Redis INCR은 되돌려지지 않았다.

```
Redis count:   100  (79개는 DB 실패했는데 decrement 안 됨)
coupon_issue:   21  (실제 발급)
```

현재 코드에서 DB 실패 시 Redis를 보상하는 로직이 없다. 이 문제는 Deadlock 해결 후 자연스럽게 해소되었지만, 설계 시 고려해야 할 포인트.

---

## 시행착오 2: redis-cli에서 키가 보이지 않는 문제

### 증상

앱은 정상 동작하지만 `redis-cli`에서 `KEYS *`가 `(empty array)`.

### 디버깅 과정

직렬화(Serializer) 문제를 의심하여 브레이크포인트로 확인:

| 확인 항목 | 결과 |
|----------|------|
| connectionFactory host/port | localhost:6379 ✅ |
| connectionFactory db | 0 ✅ |
| keySerializer | StringRedisSerializer ✅ |
| valueSerializer | StringRedisSerializer ✅ |
| defaultSerializer | JdkSerializationRedisSerializer |

key/value serializer가 String이므로 직렬화 문제는 아니었다.

### 원인

**redis-cli 접속 포트가 앱이 사용하는 Redis 포트와 달랐다.** 포트를 맞추니 정상 조회됨.

> 단순한 문제지만, "serializer 문제인가?" → "DB index 문제인가?" → "연결 대상 문제였다"로 좁혀가는 디버깅 과정 자체가 학습.

---

## 최종 테스트 결과 (외래키 제거 후)

```
✗ status is 200
  ↳  50% — ✓ 100 / ✗ 100

http_req_duration..............: avg=632.8ms
  { expected_response:true }...: avg=1.17s    p(95)=1.24s
http_req_failed................: 50.00%  100 out of 200
http_reqs......................: 200    158 req/s
success_count..................: 100
```

```sql
SELECT COUNT(*) FROM coupon_issue WHERE coupon_id = 1;  -- 100
```

```bash
redis-cli GET coupon:1:count  -- 100
```

| 지표 | 결과 |
|------|------|
| Redis count | **100** ✅ |
| coupon_issue | **100건** ✅ |
| issued_count | 7 (B안에서 미사용 — Lost Update 예상대로) |
| Redis↔DB 일치 | **일치** ✅ |
| Deadlock | **없음** ✅ |

---

## Phase 2 vs Phase 3 비교

| 지표 | Phase 2 (비관적 락) | Phase 3 (Redis INCR) | 변화 |
|------|-------------------|---------------------|------|
| 성공 | 100/200 | 100/200 | 동일 |
| 정합성 | 일치 ✅ | 일치 ✅ | 동일 |
| Deadlock | 없음 | 없음 | 동일 |
| **평균 응답 (성공)** | **4.34s** | **1.17s** | **3.7배 개선** ⚡ |
| **p95** | **5.57s** | **1.24s** | **4.5배 개선** ⚡ |
| **처리량** | **~35 req/s** | **158 req/s** | **4.5배 개선** ⚡ |
| 병목 | DB 행 락 대기 | 거의 없음 | |

### 왜 이렇게 빠른가

- **비관적 락**: 모든 요청이 DB 행 락을 순차 대기 → 직렬화 병목
- **Redis INCR**: 수량 체크가 Redis에서 원자적으로 끝남 → DB는 INSERT만 담당 → 락 경쟁 없음

---

## 이 Phase에서 배운 것

1. **Redis INCR은 수량 카운팅에 완벽하다** — 원자적이고, DB 락 없이 동시성을 해결한다.
2. **외래키(FK)는 암묵적 S Lock을 만든다** — 대규모 동시 INSERT 시 Deadlock의 원인이 된다.
3. **Redis와 DB는 별개의 시스템이다** — DB 실패 시 Redis 보상 로직이 필요하다.
4. **수량 관리의 단일 원천을 정하라** — Redis와 DB 양쪽에서 관리하면 불일치가 발생한다.

---

## Redisson 분산락

### 왜 분산락인가

Redis INCR은 "수량 카운팅"만 원자적이다. 하지만 실무에서는:

- **INCR → DB 저장** 사이에 실패하면 Redis↔DB 불일치
- **멀티 서버** 환경에서 동일 자원에 대한 동기화 필요
- **복합 연산** (체크 → 차감 → 저장)을 하나의 임계 영역으로 보호해야 하는 경우

분산락은 이런 시나리오에서 **프로세스 간 상호 배제**를 보장한다.

### 구현

```java
public void issueCouponWithRedissonLock(CouponRequest in) {
    String lockKey = "lock:coupon:" + in.getCouponId();
    RLock lock = redissonClient.getLock(lockKey);

    try {
        boolean acquired = lock.tryLock(5, 3, TimeUnit.SECONDS);
        if (!acquired) {
            throw new IllegalStateException("락 획득 실패");
        }

        // 락 안에서 Redis INCR + DB 저장
        Long count = couponRedisRepository.increment(in.getCouponId());
        if (count > 100) {
            couponRedisRepository.decrement(in.getCouponId());
            throw new IllegalStateException("발급 수량 초과");
        }

        couponService.issueCoupon(in);

    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
    } finally {
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }
}
```

### 동작 원리

```
Thread A: tryLock("lock:coupon:1") → 획득 ✅
Thread B: tryLock("lock:coupon:1") → 대기 (최대 5초)
Thread A: INCR → DB INSERT → unlock
Thread B: 락 획득 → INCR → DB INSERT → unlock
→ 완전 직렬화, 데이터 정합성 보장
```

**INCR만 쓸 때와의 차이:**

| 항목 | Redis INCR만 | 분산락 + INCR |
|------|-------------|--------------|
| 수량 체크 | 원자적 ✅ | 원자적 ✅ |
| DB 저장 실패 시 | Redis 보상 필요 | 락 안에서 일관성 보장 |
| 동시 처리 | 병렬 (빠름) | 직렬 (느리지만 안전) |
| 멀티 서버 | 동기화 없음 | 분산 동기화 ✅ |

---

### 테스트 결과

```
✗ status is 200
  ↳  50% — ✓ 100 / ✗ 100

http_req_duration..............: avg=2.03s
  { expected_response:true }...: avg=3.93s    p(95)=5.13s
http_req_failed................: 50.00%  100 out of 200
http_reqs......................: 200    ~39 req/s
success_count..................: 100
```

| 지표 | 결과 |
|------|------|
| Redis count | **100** ✅ |
| coupon_issue | **100건** ✅ |
| issued_count | **100** ✅ (락 안에서 순차 실행) |
| Redis↔DB 일치 | **일치** ✅ |
| Deadlock | **없음** ✅ |

> INCR만 쓸 때 issued_count=7이었던 것과 달리, 분산락에서는 **issued_count도 정확히 100**. 락이 coupon.issue()까지 보호하기 때문.

---

## Phase 2 vs Phase 3 전체 비교

| 지표 | Phase 2 (비관적 락) | Phase 3 INCR | Phase 3 분산락 |
|------|-------------------|-------------|---------------|
| 성공 | 100/200 | 100/200 | 100/200 |
| 정합성 | ✅ | ✅ (issued_count 제외) | ✅ (전부 일치) |
| Deadlock | 없음 | 없음 | 없음 |
| **평균 응답 (성공)** | **4.34s** | **1.17s** ⚡ | **3.93s** |
| **p95** | **5.57s** | **1.24s** ⚡ | **5.13s** |
| **처리량** | **~35 req/s** | **158 req/s** ⚡ | **~39 req/s** |
| 병목 | DB 행 락 대기 | 거의 없음 | 분산락 순차 대기 |

### 해석

- **INCR**: 가장 빠르다. 수량 체크만 Redis에서 하고 나머지는 병렬 → 4.5배 성능 향상.
- **분산락**: 비관적 락과 비슷한 성능. 락 안에서 전부 직렬화하기 때문. 다만 락의 위치가 DB → Redis로 이동.
- **분산락의 가치는 성능이 아니라 안전성**. 멀티 서버 환경에서 DB 락은 서버 간 동기화가 안 되지만, Redis 분산락은 된다.

### 언제 어떤 방식을 쓰는가

| 상황 | 추천 방식 |
|------|----------|
| 단일 서버, 단순 수량 제어 | Redis INCR |
| 멀티 서버, 높은 정합성 요구 | Redisson 분산락 |
| DB 의존성 없이 빠른 차단 | Redis INCR |
| 복합 연산 (체크 → 차감 → 저장) 보호 | Redisson 분산락 |

---

## 고부하 테스트 (5,000 req/s)

소규모 테스트(200 VU, 100장)에서는 Redis INCR과 분산락 모두 정합성을 완벽하게 보장했다.
실전에 가까운 트래픽에서 어떤 병목이 나타나는지 확인하기 위해 부하를 대폭 높였다.

### 테스트 환경 변경

| 항목 | 소규모 | 고부하 |
|------|--------|--------|
| 목표 | 200 VU × 1회 | 5,000 req/s × 20초 |
| 쿠폰 수량 | 100장 | **1,000,000장** (수량 병목 배제) |
| HikariCP | 기본값 | max-pool-size: **250** |
| k6 실행 방식 | shared-iterations | **constant-arrival-rate** |
| VU | 200 | preAllocated 3,000 / max 10,000 |

---

### 분산락 방식의 병목 — 왜 ~200 req/s에서 포화하는가

`issueCouponWithRedissonLock()`으로 5,000 req/s를 쏘면 **실처리량이 ~200 req/s 근처에서 머문다.**

#### 원인: 단일 락 직렬화

```
lock:coupon:1 (단일 분산락)
    │
    ├─ 락 획득:   Redis 1회 (~1ms)
    ├─ Redis INCR: Redis 1회 (~1ms)
    ├─ DB SELECT:  MySQL 1회 (~1ms)
    ├─ DB INSERT:  MySQL 1회 (~1ms)
    ├─ DB COMMIT:  MySQL 1회 (~1ms)
    └─ 락 해제:   Redis 1회
    ───────────────────
    합계: ~5ms/건
```

**1,000ms ÷ 5ms = 200 req/s** — 이론적 상한. 단일 락에 모든 요청이 줄을 서니, 서버를 아무리 늘려도 이 수치를 넘을 수 없다.

---

### 구조 변경 실험: A안 vs B안

병목을 완화하기 위해 두 가지 구조를 실험했다.

| 방식 | 락 범위 | DB 저장 위치 |
|------|---------|-------------|
| **A안** | 락 안: Redis INCR만 | 락 밖에서 DB 저장 |
| **B안** | 락 없음 | Redis INCR 후 바로 DB 저장 |

#### A안: 분산락 범위 축소

```java
// 락 안: Redis만
try {
    Long count = couponRedisRepository.increment(in.getCouponId());
    if (count > 1000000) { ... }
} finally {
    lock.unlock();
}
// 락 밖: DB 저장
couponService.issueCoupon(in);
```

#### B안: 분산락 완전 제거

```java
// 락 없이 Redis INCR → DB 저장
Long count = couponRedisRepository.increment(in.getCouponId());
if (count > 1000000) { ... }
couponService.issueCoupon(in);
```

#### 결과 비교

| 지표 | A안 (락 밖 DB) | B안 (락 제거) |
|------|---------------|-------------|
| 처리량 | 409 req/s | 633 req/s |
| 평균 응답 | 12s (성공: 21s) | 6.2s |
| 성공률 | 56% | 40% |
| dropped | 79,460 | 48,335 |

B안이 처리량은 높지만 성공률은 더 낮다 — 게이트키퍼(락)가 없어서 DB에 트래픽이 직접 몰리기 때문.

---

### 발견 1: tryLock Silent Success 버그

분산락 코드에서 `tryLock` 실패 시 예외를 던지지 않고 그냥 리턴하는 코드가 있었다.

```java
boolean acquired = lock.tryLock(5, TimeUnit.SECONDS);
if (!acquired) {
    // 예외를 안 던짐 → 메서드가 정상 리턴 → Controller가 200 OK 반환
    return;
}
```

**결과**: 락을 못 잡은 요청도 200 OK로 응답 → 클라이언트는 성공으로 인식하지만 실제로는 아무 처리 안 됨.

**수정**: `throw new IllegalStateException("락 획득 실패")` 추가.

---

### 발견 2: coupon.issue()의 Lost Update

A안/B안 공통으로 **coupon 테이블의 issued_count만 실제와 다르게 나타났다.**

| 지표 | A안 | B안 |
|------|-----|-----|
| coupon.issued_count | 11,352 | 266 |
| coupon_issue 행 수 | 11,593 | 6,980 |
| Redis count | 11,593 | 6,980 |
| **issued_count 손실** | **241건** | **6,714건** |

`coupon_issue` 행 수와 Redis 값은 항상 정확히 일치한다. 문제는 오직 `coupon.issue()` — `issuedCount++`.

#### 원인: JPA Read-Modify-Write

```java
public void issue() {
    this.issuedCount++;  // ← 문제의 코드
}
```

```
Thread A: SELECT issuedCount → 10
Thread B: SELECT issuedCount → 10  (같은 값을 읽음)
Thread A: issuedCount = 11 → UPDATE
Thread B: issuedCount = 11 → UPDATE  (A의 증가가 덮어씌워짐)
→ 2번 발급했지만 1만 증가 = Lost Update
```

A안은 락 밖에서 DB 저장하므로 일부만 충돌(241건 손실). B안은 완전 병렬이므로 대량 충돌(6,714건 손실).

---

### 해결: coupon.issue() 제거

Redis가 수량의 단일 진실 원천이므로, `coupon.issue()`(issuedCount++)를 아예 제거.

**Before:**
```java
@Transactional
public void issueCoupon(CouponRequest in) {
    Coupon coupon = couponRepository.findById(couponId).orElseThrow(...);
    coupon.issue();  // issuedCount++ → Lost Update 발생
    couponIssueRepository.save(newIssue);
}
```

**After:**
```java
@Transactional
public void issueCoupon(CouponRequest in) {
    couponRepository.findById(couponId).orElseThrow(...);  // 존재 확인만
    couponIssueRepository.save(CouponIssue.create(couponId, userId));
}
```

DB의 `issued_count` 컬럼은 더 이상 관리하지 않는다. 수량 확인이 필요하면 `Redis GET coupon:{id}:count` 또는 `SELECT COUNT(*) FROM coupon_issue`를 사용.

---

### 제거 후 재테스트 — 새로운 병목 발견

`coupon.issue()` 제거 후, 락 없는 Redis INCR 방식(`issueCouponWithRedis`)으로 동일 조건 재테스트.

```
✗ status is 200
  ↳  21% — ✓ 3,772 / ✗ 13,655
✗ response time < 500ms
  ↳  64% — ✓ 11,327 / ✗ 6,100

dropped_iterations.............: 82,598
http_req_duration..............: avg=1.39s   p(90)=6.16s   p(95)=7.03s
  { expected_response:true }...: avg=2s      p(90)=5.54s   p(95)=6.84s
http_req_failed................: 78.35%  13,655 out of 17,427
http_reqs......................: 17,427  515 req/s
success_count..................: 3,772   112 req/s
vus_max........................: 4,616
```

| 지표 | 결과 |
|------|------|
| 전송 | 17,427 / 100,000 (17.4%) |
| 성공 | 3,772 (21.6%) |
| 실패 | 13,655 (78.4%) |
| dropped | 82,598 |
| 처리량 | 515 req/s |

**Lost Update는 해결했지만 78%가 실패** — 왜?

#### 원인: DB 커넥션 풀 + Tomcat 스레드 포화

```
5,000 req/s 도착
    ↓
Redis INCR (~0.5ms, 거의 무제한 통과) ← 게이트키퍼 없음
    ↓
전부 DB로 몰림
    ↓
┌──────────────────────────────────┐
│ Tomcat 스레드 풀: 200개 (기본값)    │ ← 첫 번째 병목
│ HikariCP 커넥션 풀: 250개          │ ← 두 번째 병목
└──────────────────────────────────┘
```

1. Redis INCR은 ~0.5ms로 거의 즉시 통과 — **속도 제한 기능 없음**
2. 5,000 req/s가 그대로 DB에 쏟아짐
3. Tomcat 200 스레드 즉시 소진 → accept queue(100) 초과 → **연결 거부**
4. HikariCP 250 커넥션 소진 → 대기 → 타임아웃 → **500 에러**
5. DB 예외 → catch에서 Redis decrement + re-throw → 실패 응답

**역설**: 분산락이 있을 때는 락이 자연스러운 **트래픽 조절기** 역할을 했다. 락을 제거하니 처리량은 올랐지만, DB 인프라가 감당 못하는 트래픽이 유입됨.

---

### 고부하 테스트 종합 비교

| 지표 | 분산락 (이론) | A안 (락 밖 DB) | B안 (락 제거) | 최종 (issue() 제거) |
|------|-------------|---------------|-------------|-------------------|
| 처리량 | ~200 req/s | 409 req/s | 633 req/s | 515 req/s |
| 성공률 | — | 56% | 40% | 21.6% |
| Lost Update | 없음 | 241건 | 6,714건 | **없음** ✅ |
| 데이터 정합성 | ✅ | ❌ | ❌ | ✅ |
| 핵심 병목 | 단일 락 직렬화 | 락 직렬화 + DB | DB 커넥션 풀 | DB 커넥션 풀 |

**결론**: 동기 처리 구조에서는 어떤 방식을 쓰든 결국 DB가 병목이 된다.

- 락을 쓰면 → 락이 병목 (200 req/s 상한)
- 락을 빼면 → DB 커넥션 풀이 병목 (78% 실패)
- **근본 해결: DB 저장을 요청 경로에서 분리해야 한다** → Phase 4 (Kafka)

---

## 이 Phase에서 배운 것 (전체)

1. **Redis INCR은 수량 카운팅에 완벽하다** — 원자적이고, DB 락 없이 동시성을 해결한다.
2. **외래키(FK)는 암묵적 S Lock을 만든다** — 대규모 동시 INSERT 시 Deadlock의 원인이 된다.
3. **Redis와 DB는 별개의 시스템이다** — DB 실패 시 Redis 보상 로직이 필요하다.
4. **수량 관리의 단일 진실 원천을 정하라** — Redis와 DB 양쪽에서 관리하면 Lost Update와 불일치가 발생한다.
5. **분산락은 성능이 아닌 안전성을 위한 도구** — 직렬화로 느려지지만, 멀티 서버에서 정합성을 보장한다.
6. **분산락은 의도치 않은 트래픽 조절기** — 락이 있으면 DB가 보호되지만, 제거하면 DB에 트래픽이 직격한다.
7. **JPA의 Read-Modify-Write는 동시성에 취약하다** — `entity.field++` 패턴은 Lost Update를 일으킨다.
8. **동기 처리의 한계** — 요청-응답 사이클에 DB 저장이 포함되면, 트래픽이 늘수록 DB 커넥션 풀이 병목이 된다.

---

## 다음: Phase 4 — Kafka 비동기 처리

Phase 3에서 확인한 핵심 문제: **동기 처리 구조에서는 DB가 결국 병목이 된다.**

- 락을 쓰면 → 200 req/s 상한 (단일 락 직렬화)
- 락을 빼면 → DB 커넥션 풀 포화 → 78% 실패

**해결 방향**: 요청 경로에서 DB 저장을 분리한다.

```
현재 (동기):    요청 → Redis INCR → DB INSERT → 응답  (DB가 응답 속도를 결정)
Phase 4 (비동기): 요청 → Redis INCR → Kafka 발행 → 즉시 응답  (DB 부하와 무관)
                                        ↓
                               Consumer → DB INSERT  (자기 페이스대로)
```

Redis INCR로 "발급 가능 여부"를 즉시 판단하고, DB 저장은 Kafka Consumer가 별도 속도로 처리한다.
→ 응답 속도 ↑, DB 부하 ↓, 시스템 안정성 ↑
