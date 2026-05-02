# Phase 6 체크리스트 (모니터링 + 가시화)

## 1) 환경 고정

- [ ] 로컬 8080 앱 종료(consumer group 혼선 방지)
- [ ] `app1/app2/app3/nginx/kafka/redis/mysql/prometheus/grafana/kafka-exporter` 모두 실행 확인
- [ ] Nginx 모드(`default` 또는 `bench`) 하나로 고정

## 2) 사전 검증

- [ ] `http://localhost:9090/targets`에서 app, kafka-exporter 타겟 `UP` 확인
- [ ] Grafana 대시보드 로딩 확인
- [ ] `coupon-issue-group` lag 초기값 확인(가능하면 낮은 상태에서 시작)

## 3) 실험 시나리오 실행

- [ ] rate `3500`, `3600` 각각 3회 실행
- [ ] 실행 명령:

```powershell
powershell -ExecutionPolicy Bypass -File "scripts/run-k6-rate-matrix.ps1" `
  -TargetUrl "http://localhost:8081/api/v1/coupons/issue" `
  -RatesCsv "3500,3600" `
  -Repeats 3
```

- [ ] 각 run 전 `coupon_issue`/Redis 초기화 옵션 유지(스크립트 기본)

## 4) 관측 지표 수집 (동일 시간축)

- [ ] HTTP RPS
- [ ] HTTP p95 latency
- [ ] HTTP 5xx ratio
- [ ] dropped iterations (k6 결과)
- [ ] Kafka total lag
- [ ] Kafka partition lag
- [ ] Hikari active connections

## 5) Lag 회복성 측정

- [ ] 각 run 종료 직후 lag drain 속도 측정

```powershell
powershell -ExecutionPolicy Bypass -File "scripts/measure-lag-drain.ps1" -IntervalSeconds 5 -Samples 8
```

- [ ] `drainPerSec` 기록
- [ ] lag 0 근처 복귀 시간 기록

## 6) 결과 판정

- [ ] `failed%`, `p95`, `dropped`, `req/s` 기준으로 rate별 우선순위 결정
- [ ] 안정 처리량(운영 권장 rate) 1개 확정
- [ ] 포화 시작 지점(rate) 1개 확정

## 7) 문서화

- [ ] Phase 6 결과 문서 초안 작성(표 + 캡처 + 해석)
- [ ] 캡처 4장 첨부: RPS / p95 / total lag / partition lag
- [ ] 결론 3줄 고정(운영 rate, 포화 rate, 병목 위치)
