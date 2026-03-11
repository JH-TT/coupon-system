# coupon-system

선착순 쿠폰 발급 시스템 — 대규모 트래픽 학습 프로젝트

## 학습 방식

기본 구현 → 부하 테스트 → 문제 발견 → 해결 → 비교 측정의 반복 사이클.
처음부터 완성형이 아닌, 단계별로 문제를 체감하며 성장시키는 프로젝트.

## Phase 로드맵

| Phase | 핵심 | 기술 |
|-------|------|------|
| 1 | Naive 구현 + Race Condition 체감 | Spring Boot + MySQL |
| 2 | DB 락 전략 비교 (비관적/낙관적/네임드) | JPA Locking |
| 3 | Redis 원자적 연산 + 분산락 | Redis + Redisson |
| 4 | 비동기 처리 + 메시지 큐 | Kafka |
| 5 | Scale Out + 로드밸런싱 | Nginx + Docker |
| 6 | 모니터링 + 가시화 | Prometheus + Grafana |
| 7 | 방어적 설계 | Resilience4j + Bucket4j |

## 기술 스택

- Java 17, Spring Boot 3.5.0, Gradle Kotlin DSL
- MySQL 8 (Docker), H2 (Test)
- k6 (부하 테스트)

## 빌드 & 실행

```bash
# 인프라 (MySQL)
docker compose up -d

# 빌드
./gradlew.bat compileJava

# 테스트
./gradlew.bat test

# 실행
./gradlew.bat bootRun

# 부하 테스트 (k6 설치 필요)
k6 run k6/issue-coupon.js
```

## 핵심 API

```
POST /api/v1/coupons/issue   — 쿠폰 발급 (핵심)
GET  /api/v1/coupons/{id}    — 쿠폰 조회
```

## 컨벤션

- 패키지: 도메인 기반 (`domain/coupon/`, `service/`, `controller/`)
- JPA: `validate` 모드 (스키마는 SQL로 관리)
- DTO: Request/Response 분리
- 테스트: JUnit 5 + H2

## 매 Phase 필수 루틴

1. 현재 방식으로 구현
2. k6 부하 테스트 → 문제 발견 (숫자로 기록)
3. 원인 분석 → "왜?"를 3번 반복
4. 해결 방안 적용
5. 동일 조건 부하 테스트 → Before/After 비교
