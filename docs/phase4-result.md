# Phase 4: Kafka 비동기 처리 — 응답 경로에서 DB 분리

## 목표

Phase 3에서 확인한 "동기 처리의 한계"(DB 커넥션 풀 포화)를 해결한다.
요청 경로에서 DB 저장을 분리하여, 응답 속도와 처리량을 동시에 개선한다.

---

## 테스트 환경

| 항목 | 설정 |
|------|------|
| 쿠폰 수량 | 1,000,000장 (기본) / 1,000장 (제한 테스트) |
| 부하 도구 | k6 (`constant-arrival-rate`) |
| DB | MySQL 8 (Docker) |
| Redis | Redis 7 (Docker, port 6380) |
| Kafka | Confluent cp-kafka 7.7.1 (Docker, KRaft 모드) |
| 앱 | Spring Boot 3.5.0 + spring-kafka 3.3.6 |
| HikariCP | max-pool-size: 250 |

---

## 설계: 비동기 분리 아키텍처

### Phase 3 (동기) vs Phase 4 (비동기)

```
[Phase 3 — 동기]
요청 → Redis INCR → DB SELECT + INSERT → 응답
                     ^^^^^^^^^^^^^^^^
                     병목: DB 커넥션 풀 포화

[Phase 4 — 비동기]
요청 → Redis INCR → Kafka 발행 → 즉시 응답      ← API 경로 (빠름)
                         ↓
                    Consumer → DB INSERT           ← 백그라운드 (자기 페이스)
```

- Redis INCR: 수량의 단일 진실 원천 (Phase 3에서 확정)
- Kafka Producer: Redis INCR 성공 시 발급 이벤트 발행
- Kafka Consumer: 이벤트를 수신하여 DB INSERT 처리
- API 응답 경로에 DB가 없으므로 커넥션 풀 포화 문제 해소

---

## 구현

### Kafka Producer

```java
@Component
@RequiredArgsConstructor
public class CouponIssueProducer {

    private static final String TOPIC = "coupon-issue";
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(Long couponId, Long userId) throws JsonProcessingException {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("couponId", couponId);
        node.put("userId", userId);
        String json = objectMapper.writeValueAsString(node);

        kafkaTemplate.send(TOPIC, String.valueOf(couponId), json);
    }
}
```

### Kafka Consumer

```java
@Component
@RequiredArgsConstructor
public class CouponIssueConsumer {

    private final CouponService couponService;
    private final ObjectMapper mapper;

    @KafkaListener(topics = "coupon-issue", groupId = "coupon-issue-group")
    public void consume(String message) throws JsonProcessingException {
        Map map = mapper.readValue(message, Map.class);

        CouponRequest reqIn = new CouponRequest();
        reqIn.setCouponId(Long.parseLong(String.valueOf(map.get("couponId"))));
        reqIn.setUserId(Long.parseLong(String.valueOf(map.get("userId"))));

        couponService.issueCoupon(reqIn);
    }
}
```

### Facade (Kafka 경로)

```java
public boolean issueCouponWithKafka(CouponRequest in) throws Exception {
    Long count = couponRedisRepository.increment(in.getCouponId());

    if (count > 1000) {
        couponRedisRepository.decrement(in.getCouponId());
        return false;  // 초과 — 예외 없이 리턴
    }

    producer.send(in.getCouponId(), in.getUserId());
    return true;
}
```

---

## 부하 테스트 결과

### 테스트 1: 초당 10건 × 10초 (약 100건)

| 항목 | 값 |
|------|------|
| 설정 | rate=10/s, duration=10s, preAllocatedVUs=10, maxVUs=50 |
| 총 요청 | 101건 |
| 실패율 | 0% |
| 응답시간 avg | 10.22ms |
| 응답시간 p95 | 8.19ms |
| 응답시간 max | 273.75ms |
| 500ms 미만 | 100% |

### 테스트 2: 초당 100건 × 20초 (약 2,000건)

| 항목 | 값 |
|------|------|
| 설정 | rate=100/s, duration=20s, preAllocatedVUs=50, maxVUs=150 |
| 총 요청 | 2,001건 |
| 실패율 | 0% |
| 응답시간 avg | 5.08ms |
| 응답시간 p95 | 4.57ms |
| 응답시간 max | 314.25ms |
| 500ms 미만 | 100% |
| 정합성 | Redis 키 수, coupon_issue 테이블 row 수 모두 2,001건으로 일치 |

### 테스트 3: 초당 2,000건 × 10초 (약 20,000건, 제한 없음)

| 항목 | 값 |
|------|------|
| 설정 | rate=2000/s, duration=10s, preAllocatedVUs=1000, maxVUs=2000 |
| 총 요청 | 19,345건 (dropped: 662) |
| 실패율 | 0% |
| 응답시간 avg | 70.26ms |
| 응답시간 p95 | 766.36ms |
| 응답시간 max | 939.63ms |
| 500ms 미만 | 93% |
| 실제 발급 | 21,346건 |

### 테스트 4: 초당 2,000건 × 10초 (최대 수량 1,000개 제한)

| 항목 | 값 |
|------|------|
| 설정 | rate=2000/s, duration=10s, preAllocatedVUs=1000, maxVUs=2000 |
| 총 요청 | 9,035건 (dropped: 10,308) |
| 성공(200) | 1,000건 |
| 실패 | 8,035건 (88.93%) |
| 응답시간 avg | 736.64ms |
| 응답시간 p95 | 3.1s |
| 응답시간 max | 3.54s |
| 500ms 미만 | 63% |
| 정합성 | Redis 키 수, coupon_issue 모두 정확히 1,000건 |

**문제 발견**: Kafka를 거치지 않고 바로 reject하는 요청이 오히려 느림.

---

## 후속 실험: 응답 지연 원인 추적

테스트 4에서 거절 응답이 느린 원인을 추적하기 위해, 예외 처리 방식을 변경하며 재실험했다.

### 원인 분석

거절 경로: `Redis INCR → Redis DECR → throw IllegalStateException → Spring 기본 에러 처리`

Spring 기본 에러 처리의 문제:
1. **이중 디스패치**: 예외 → `DispatcherServlet` → `BasicErrorController(/error)` 내부 포워드 → JSON 에러 응답 생성
2. **예외 스택 트레이스 생성**: Spring AOP 프록시 체인으로 30~50프레임 스택, 초당 2,000번 생성 시 CPU 부담
3. **연쇄 붕괴**: 거절 경로가 살짝 느려짐 → Tomcat 스레드풀 포화 → 큐 대기 → 응답시간 급증 → VU 묶임 → dropped 폭증

### 테스트 4-1: @ControllerAdvice 예외 핸들러 추가

```java
@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<String> handleIllegalState(IllegalStateException e) {
        return ResponseEntity.status(409).body(e.getMessage());
    }
}
```

| 항목 | 테스트 4 (기본 에러) | 테스트 4-1 (@ControllerAdvice) |
|------|------|------|
| 총 요청 | 9,035 | 19,336 |
| dropped | 10,308 | 665 |
| avg | 736.64ms | 109.46ms |
| p95 | 3.1s | 849.2ms |
| max | 3.54s | 995.84ms |
| 500ms 미만 | 63% | 93% |

**결과**: `@ControllerAdvice` 추가만으로 dropped 10,308 → 665, max 3.54s → 995ms로 대폭 개선. Spring 기본 에러 처리(`/error` 포워딩)가 병목이었음을 확인.

### 테스트 4-2: 예외를 아예 안 던지기

Facade에서 boolean 리턴, Controller에서 false면 409 응답 직접 반환.

| 항목 | 테스트 4-1 (@ControllerAdvice) | 테스트 4-2 (예외 미사용) |
|------|------|------|
| 총 요청 | 19,336 | 18,534 |
| dropped | 665 | 1,469 |
| avg | 109.46ms | 203.88ms |
| p95 | 849.2ms | 917.89ms |
| max | 995.84ms | 1.05s |

**1회 실행 기준**으로는 4-2가 오히려 느렸으나, 이후 5회 반복 실험에서 이는 측정 편차임을 확인.

---

## 안정성 검증: 5회 반복 테스트

1회 실행 결과가 콜드 스타트 등 외부 요인에 좌우되므로, 동일 조건(2,000 req/s, 10s, 최대 1,000개)으로 5회 반복 실행. 5회차는 서버 재시작 후 실행.

### 방법 1: @ControllerAdvice (5회)

| 회차 | 총 요청 | dropped | avg (전체) | avg (성공) | p95 | max | 500ms 미만 |
|------|---------|---------|-----------|-----------|-----|-----|-----------|
| 1회 | 19,054 | 947 | 117.6ms | 1,010ms | 925.64ms | 1.12s | 93% |
| 2회 | 20,001 | 0 | 38.97ms | 7.66ms | 174.13ms | 302.43ms | 100% |
| 3회 | 20,001 | 0 | 7.44ms | 13.6ms | 19.6ms | 108.37ms | 100% |
| 4회 | 20,000 | 0 | 6.74ms | 16.48ms | 17.9ms | 91.3ms | 100% |
| 5회 (재시작) | 19,355 | 646 | 91.89ms | 846.09ms | 764.86ms | 972.91ms | 93% |

### 방법 2: 예외 미사용 (5회)

| 회차 | 총 요청 | dropped | avg (전체) | avg (성공) | p95 | max | 500ms 미만 |
|------|---------|---------|-----------|-----------|-----|-----|-----------|
| 1회 | 19,156 | 846 | 127.22ms | 1,000ms | 924.58ms | 1.12s | 93% |
| 2회 | 20,011 | 0 | 14.12ms | 6.77ms | 65.49ms | 174.85ms | 100% |
| 3회 | 20,001 | 0 | 7.85ms | 16.7ms | 22.41ms | 161.29ms | 100% |
| 4회 | 20,000 | 0 | 5.8ms | 5.69ms | 16.15ms | 76.73ms | 100% |
| 5회 (재시작) | 19,353 | 649 | 116.39ms | 871.12ms | 815.14ms | 976.9ms | 93% |

### 워밍업 후(2~4회차) 비교

| 지표 | 방법 1 (avg) | 방법 2 (avg) |
|------|-------------|-------------|
| 전체 avg | 17.72ms | 9.26ms |
| 성공 요청 avg | 12.58ms | 9.72ms |
| max | 167.37ms | 137.62ms |
| dropped | 0 | 0 |

**결론**: 워밍업 이후에는 두 방법 모두 안정적이며, 유의미한 성능 차이 없음.

### 콜드 스타트 원인 분석

1회차/5회차(서버 재시작)에서 일관되게 성능이 저하되는 원인:

| 원인 | 영향 시점 | 설명 |
|------|----------|------|
| JIT 컴파일 | 수천 호출까지 | 핫 메서드가 interpreted → native 코드로 컴파일되기 전까지 느림 |
| Kafka Producer 초기 연결 | 첫 send() | 브로커 TCP 연결 + 메타데이터 fetch + 버퍼 할당 (200~500ms) |
| HikariCP 커넥션 풀 | 첫 DB 접근 | minimum-idle=5, 나머지 245개를 동적 생성 |
| Redis 커넥션 | 첫 Redis 호출 | Lettuce 커넥션 생성 + 인증 |

---

## 한계 테스트: 초당 5,000건

워밍업 후 2,000/s에서 안정적이었으므로, 부하를 5,000/s로 올려 한계점을 확인.

설정: rate=5000/s, duration=10s, preAllocatedVUs=2000, maxVUs=5000

### 콜드 스타트 1회차

| 항목 | 방법 1 (@ControllerAdvice) | 방법 2 (예외 미사용) |
|------|--------------------------|-------------------|
| 총 요청 | 25,960 | 24,255 |
| dropped | 24,021 | 25,692 |
| 성공(200) | 1,000 | 1,000 |
| avg | 1.07s | 1.36s |
| p95 | 1.28s | 1.62s |
| max | 1.39s | 1.66s |
| 500ms 미만 | 5% | 1% |

### 워밍업 후

| 항목 | 방법 1 | 방법 2 (1회) | 방법 2 (2회) |
|------|--------|-------------|-------------|
| 총 요청 | 30,398 | 29,243 | 28,778 |
| dropped | 19,561 | 20,756 | 21,221 |
| 성공(200) | 1,000 | 1,000 | 1,000 |
| avg | 759.87ms | 708.14ms | 705.37ms |
| avg (성공) | 107.32ms | 87.01ms | 95.56ms |
| p95 | 1s | 1.1s | 923.56ms |
| max | 1.19s | 1.21s | 3.04s |
| 500ms 미만 | 9% | 27% | 14% |

**결론**: 5,000 req/s에서는 두 방법 모두 서버 포화. dropped 19k~21k, 거절 응답도 700ms~1s 소요. 이는 코드 최적화로 해결할 수 없는 영역이며, **Tomcat 큐 대기**가 지배적 요인.

---

## Phase 3 vs Phase 4 비교

| 지표 | Phase 3 (Redis + 동기 DB) | Phase 4 (Redis + Kafka) |
|------|-------------------------|------------------------|
| 2,000/s avg (워밍업 후) | 70ms | **6~17ms** |
| 2,000/s 성공률 | 21.6% (DB 커넥션 풀 포화) | **100%** |
| 2,000/s dropped | 662 | **0** |
| Lost Update | 있었음 | **없음** |
| 병목 지점 | DB 커넥션 풀 | Tomcat 스레드 (5,000/s부터) |
| 수량 정합성 | Redis + DB 이중 관리 불일치 가능 | Redis 단일 원천, Kafka Consumer가 DB 반영 |

---

## 발견한 교훈

1. **비동기 분리의 효과**: DB를 응답 경로에서 빼는 것만으로 성공률 21.6% → 100%, 응답시간 70ms → 6~17ms.
2. **Spring 기본 에러 처리는 고부하에서 병목**: `/error` 이중 디스패치가 연쇄 붕괴를 유발. `@ControllerAdvice`로 직접 처리하면 해소.
3. **예외 처리 방식(throw vs return)은 워밍업 후 성능 차이 없음**: 둘 다 동일 수준. 코드 가독성 기준으로 선택하면 됨.
4. **콜드 스타트는 실제 성능의 10~20배 느림**: JIT, Kafka 초기 연결, 커넥션 풀 초기화가 동시에 작용. 부하 테스트 시 반드시 워밍업 후 측정.
5. **부하 테스트는 최소 3회 이상 반복**: 1회 결과는 콜드 스타트/GC/OS 상태에 좌우됨. 워밍업 후 안정 구간(2~4회차) 기준으로 비교해야 공정.
6. **병목은 이동한다**: DB 병목 해소 → Tomcat 스레드/큐가 새 병목. 시스템 최적화는 병목 지점을 찾아 하나씩 해소하는 과정.

---

## 추가 실험: Kafka vs @Async

동일 조건(쿠폰 1,000개 선착순, 초과 요청 409, 약 2,700~3,100 RPS)에서 Kafka와 `@Async`를 비교했다.

### 비교 결과 요약

- 두 방식 모두 정확히 1,000건 발급(정합성 동일)
- 전체 응답시간(avg/p95)은 거의 차이 없음
  - Kafka: avg 약 705~759ms, p95 약 900ms~1.1s
  - @Async: avg 약 688~700ms, p95 약 900ms
- 성공 요청 latency는 Kafka가 더 빠름
  - Kafka: 약 87~107ms
  - @Async: 약 175~321ms
- 처리량은 둘 다 약 90건/sec 수준으로 유사
- 실패율 96~97%는 대부분 수량 초과 409(정상 거절)

### 해석

1. **전체 응답 성능은 비슷**: 포화 구간에서는 거절 응답 비중이 커서 전체 평균이 수렴한다.
2. **실제 처리 latency는 Kafka 우위**: 소비 경로에서 Kafka가 더 안정적으로 빠른 값을 보였다.
3. **최종 처리량 병목은 비동기 방식 자체가 아님**: DB/락/트랜잭션 구조가 상한을 결정한다.

### 핵심 결론

- Kafka와 `@Async`는 전체 응답 성능이 유사하다.
- Kafka는 실제 처리 latency 관점에서 더 효율적이다.
- 최종 처리량은 비동기 방식 선택보다 DB/락/트랜잭션 구조에 의해 제한된다.

---

## 추가 개선: Kafka 보상 트랜잭션 적용

Kafka 경로를 운영 관점에서 더 안전하게 만들기 위해, 실패 구간별 보상 규칙을 명확히 적용했다.

### 1) Producer 발행 실패 보상

기존 흐름: `Redis INCR -> validation -> producer.send`

개선 사항:
- `producer.send()` 실패를 즉시 감지
- 실패 시 `redis.decrement()`로 즉시 보상 후 예외 전파

의미: "수량은 증가했는데 이벤트는 발행되지 않음" 상태를 방지.

### 2) Consumer 실패 보상 (1회 보장)

개선 사항:
- 소비 실패 시 Redis 보상(`decrement`) 수행
- 동일 메시지 재시도에서 중복 보상을 막기 위해 Redis에 보상 마커(`SETNX + TTL`) 저장
- 마커가 이미 있으면 보상 생략

의미: 재시도 환경에서 과도한 감소(과보상) 방지.

### 3) 중복키는 idempotent success 처리

`uk_coupon_user` 위반은 "이미 처리된 이벤트"로 간주하고 재시도 루프에 넣지 않도록 처리했다.

의미: 같은 이벤트의 무한 재처리/seek 반복을 줄이고 소비 안정성 확보.

### 4) DB 유니크 제약 복구

`coupon_issue`에 아래 유니크 제약을 복구했다.

```sql
CONSTRAINT uk_coupon_user UNIQUE (coupon_id, user_id)
```

의미: 애플리케이션 로직과 무관하게 DB가 최종 정합성(중복 발급 방지)을 강제.

---

## 운영 시 주의사항

- Kafka Consumer 로그는 기본적으로 성공마다 찍히지 않을 수 있다.
- 현재 코드는 "중복 무시" 또는 "보상" 케이스 중심으로 로그가 남으므로, 정상 소비 추적이 필요하면 성공 로그를 별도로 추가해야 한다.

---

## 다음: Phase 5 — Scale Out + 로드밸런싱

Phase 4에서 확인한 throughput ceiling: **2,000~5,000 req/s 사이**.
5,000 req/s에서는 단일 인스턴스의 Tomcat 스레드풀이 포화되어 코드 최적화로는 더 이상 개선 불가.

**해결 방향**: 애플리케이션 인스턴스를 다중화하고, Nginx 로드밸런서로 트래픽을 분산한다.

```
현재 (단일):     k6 → [App 1대] → Redis / Kafka / MySQL
Phase 5 (다중):  k6 → [Nginx LB] → [App N대] → Redis / Kafka / MySQL
```

서버 수를 늘려 Tomcat 스레드풀 한계를 수평 확장으로 극복한다.
