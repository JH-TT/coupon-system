# 선착순 쿠폰 발급 시스템 — Phase 1 프로젝트 세팅

## 프로젝트 목표

대규모 트래픽 처리를 **단계적으로 학습**하기 위한 프로젝트.
처음부터 완성형이 아닌, 기본 구현 → 부하 테스트 → 문제 발견 → 해결의 반복 사이클로 성장시킨다.

## Phase 로드맵

| Phase | 핵심 | 기술 |
|-------|------|------|
| **1** | **Naive 구현 + Race Condition 체감** | **Spring Boot + MySQL** |
| 2 | DB 락 전략 비교 (비관적/낙관적/네임드) | JPA Locking |
| 3 | Redis 원자적 연산 + 분산락 | Redis + Redisson |
| 4 | 비동기 처리 + 메시지 큐 | Kafka |
| 5 | Scale Out + 로드밸런싱 | Nginx + Docker |
| 6 | 모니터링 + 가시화 | Prometheus + Grafana |
| 7 | 방어적 설계 | Resilience4j + Bucket4j |

---

## 기술 스택

- **Java 17** (LTS)
- **Spring Boot 3.5.0**
- **Gradle Kotlin DSL** (build.gradle.kts)
- **MySQL 8** (Docker)
- **H2** (테스트용 인메모리 DB)
- **k6** (부하 테스트)

## 의존성 (build.gradle.kts)

```kotlin
dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-web")
    compileOnly("org.projectlombok:lombok")
    runtimeOnly("com.mysql:mysql-connector-j")
    annotationProcessor("org.projectlombok:lombok")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("com.h2database:h2")
}
```

---

## 1단계: 프로젝트 생성

Spring Initializr(start.spring.io)로 생성:
- Type: Gradle - Kotlin DSL
- Language: Java
- Spring Boot: 3.5.0
- Group: com.example
- Artifact: coupon-system
- Package: com.example.couponsystem
- Java: 17
- Dependencies: Web, Data JPA, MySQL Driver, Lombok, Validation

---

## 2단계: Docker Compose 설정 (MySQL)

### docker-compose.yml

```yaml
services:
  mysql:
    image: mysql:8.0
    container_name: coupon-mysql
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: coupon
      MYSQL_USER: coupon
      MYSQL_PASSWORD: coupon
    volumes:
      - mysql-data:/var/lib/mysql
      - ./init/schema.sql:/docker-entrypoint-initdb.d/schema.sql
    command: --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci

volumes:
  mysql-data:
```

### 실행

```bash
# Docker Desktop이 실행 중인 상태에서
docker compose up -d
```

> ⚠️ "docker daemon is not running" 에러 시 → Docker Desktop 먼저 실행 후 재시도

---

## 3단계: MySQL 수동 설정

Docker Compose의 `MYSQL_DATABASE`, `MYSQL_USER` 환경변수가 자동 생성 안 될 수 있음.
이 경우 root로 접속 후 직접 생성:

### 데이터베이스 생성

```sql
CREATE DATABASE coupon DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 유저 생성 + 권한 부여

```sql
CREATE USER 'coupon'@'%' IDENTIFIED BY 'coupon';
GRANT ALL PRIVILEGES ON coupon.* TO 'coupon'@'%';
FLUSH PRIVILEGES;
```

---

## 4단계: IntelliJ Database 연결

IntelliJ 우측 **Database** 탭 → **+** → **Data Source** → **MySQL**

| 항목 | 값 |
|------|-----|
| Host | `localhost` |
| Port | `3306` |
| Database | `coupon` |
| User | `coupon` |
| Password | `coupon` |

- 드라이버 없으면 **Download missing driver files** 클릭
- **Test Connection** → 초록색 체크 확인 → Apply

---

## 5단계: application.yml 설정

### 메인 (src/main/resources/application.yml)

```yaml
spring:
  application:
    name: coupon-system

  datasource:
    url: jdbc:mysql://localhost:3306/coupon?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Seoul
    username: coupon
    password: coupon
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 10
      minimum-idle: 5
      connection-timeout: 3000

  jpa:
    hibernate:
      ddl-auto: validate
    properties:
      hibernate:
        format_sql: true
        dialect: org.hibernate.dialect.MySQLDialect
    show-sql: true
    open-in-view: false

server:
  port: 8080

logging:
  level:
    org.hibernate.SQL: DEBUG
    org.hibernate.type.descriptor.sql.BasicBinder: TRACE
```

### 테스트 (src/test/resources/application.yml)

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:coupon;MODE=MYSQL
    username: sa
    password:
    driver-class-name: org.h2.Driver

  jpa:
    hibernate:
      ddl-auto: create-drop
    properties:
      hibernate:
        format_sql: true
    show-sql: true
    open-in-view: false
```

---

## 6단계: DB 스키마 (init/schema.sql)

```sql
CREATE TABLE IF NOT EXISTS coupon (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    total_quantity INT NOT NULL,
    issued_count INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS coupon_issue (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    coupon_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    issued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_coupon_issue_coupon FOREIGN KEY (coupon_id) REFERENCES coupon(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Phase 1 테스트용 샘플 데이터
INSERT INTO coupon (name, total_quantity, issued_count) VALUES ('선착순 100명 할인 쿠폰', 100, 0);
```

> IntelliJ Database 콘솔에서 이 SQL을 직접 실행해서 테이블 생성

---

## 7단계: k6 부하 테스트 스크립트 (k6/issue-coupon.js)

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const successCount = new Counter('success_count');
const failCount = new Counter('fail_count');

export const options = {
  scenarios: {
    spike: {
      executor: 'shared-iterations',
      vus: 1000,           // 동시 사용자 1000명
      iterations: 1000,    // 총 1000번 요청
      maxDuration: '30s',
    },
  },
};

export default function () {
  const userId = __VU;

  const payload = JSON.stringify({
    userId: userId,
    couponId: 1,
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const res = http.post('http://localhost:8080/api/v1/coupons/issue', payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'response time < 500ms': (r) => r.timings.duration < 500,
  });

  if (res.status === 200) {
    successCount.add(1);
  } else {
    failCount.add(1);
  }
}
```

### 실행 (구현 완료 후)

```bash
# k6 설치: https://k6.io/docs/get-started/installation/
k6 run k6/issue-coupon.js
```

---

## 프로젝트 구조

```
coupon-system/
├── build.gradle.kts
├── docker-compose.yml
├── init/
│   └── schema.sql
├── k6/
│   └── issue-coupon.js
├── CLAUDE.md
└── src/
    ├── main/
    │   ├── java/com/example/couponsystem/
    │   │   └── CouponSystemApplication.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── resources/
            └── application.yml
```

---

## 다음 할 것 (Phase 1 구현)

직접 구현해야 할 코드:

1. **Entity**: `Coupon`, `CouponIssue` (schema.sql 구조에 맞춰서)
2. **Repository**: `CouponRepository`, `CouponIssueRepository`
3. **Service**: `CouponService` — 핵심 발급 로직
   - `SELECT issued_count` → `if < total_quantity` → `INSERT coupon_issue + UPDATE issued_count`
   - **의도적으로 동시성 제어 없이** 구현
4. **Controller**: `POST /api/v1/coupons/issue`
5. **DTO**: `CouponIssueRequest` (userId, couponId)

### 빌드 & 실행 명령어

```bash
docker compose up -d          # MySQL 시작
./gradlew.bat bootRun         # 앱 실행
k6 run k6/issue-coupon.js     # 부하 테스트
```

### Phase 1 핵심 포인트

> 일부러 동시성 제어 없이 구현한다.
> 쿠폰 100장인데 부하 테스트 돌리면 150~300장 초과 발급되는 걸 확인.
> → Race Condition이 왜 발생하는지 직접 체감하는 것이 목표.
