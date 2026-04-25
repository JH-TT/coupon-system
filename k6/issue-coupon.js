import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

function envInt(name, fallback) {
  const raw = __ENV[name];
  if (!raw) return fallback;
  const parsed = parseInt(raw, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

function resolveTargetUrl() {
  const fallback = 'http://localhost:8081/api/v1/coupons/issue';
  const raw = (__ENV.TARGET_URL || '').trim().replace(/^['"]|['"]$/g, '');

  if (!raw) return fallback;
  if (raw.startsWith('http://') || raw.startsWith('https://')) return raw;

  if (raw.startsWith('/')) {
    return `http://localhost:8081${raw}`;
  }

  return `http://${raw}`;
}

const targetUrl = resolveTargetUrl();

const successCount = new Counter('success_count');
const failCount = new Counter('fail_count');
const requestsByInstance = new Counter('requests_by_instance');

// export const options = {
//   scenarios: {
//     spike: {
//       executor: 'shared-iterations',
//       vus: 2000,           // 동시 사용자 200명
//       iterations: 2000,    // 총 1000번 요청
//       maxDuration: '10s',
//     },
//   },
// };

export const options = {
  scenarios: {
    coupon_issue_rate: {
      executor: 'constant-arrival-rate',
      rate: envInt('RATE', 5000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '10s',
      preAllocatedVUs: envInt('PRE_ALLOCATED_VUS', 2000),
      maxVUs: envInt('MAX_VUS', 5000),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.1'],   // 실패율 10% 미만 목표
    http_req_duration: ['p(95)<5000'],
  },
};

// export const options = {
//   scenarios: {
//     coupon_burst: {
//       executor: 'ramping-arrival-rate',
//       timeUnit: '1s',
//       preAllocatedVUs: 300,
//       maxVUs: 2000,
//       stages: [
//         { target: 50, duration: '5s' },   // 워밍업
//         { target: 200, duration: '5s' },  // 급상승
//         { target: 500, duration: '3s' },  // 오픈 순간 폭주
//         { target: 500, duration: '5s' },  // 폭주 유지
//         { target: 200, duration: '5s' },  // 감소
//         { target: 50, duration: '5s' },   // 정리
//         { target: 0, duration: '2s' },    // 종료
//       ],
//     },
//   },
//   thresholds: {
//     http_req_failed: ['rate<0.1'],
//     http_req_duration: ['p(95)<5000'],
//   },
// };

export default function () {
  const userId = exec.scenario.iterationInTest + 1;

  const payload = JSON.stringify({
    userId: userId,
    couponId: 1,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(targetUrl, payload, params);

  const rawInstanceHeader = res.headers['X-App-Instance'] || res.headers['x-app-instance'];
  const instance = Array.isArray(rawInstanceHeader) ? rawInstanceHeader[0] : (rawInstanceHeader || 'unknown');
  requestsByInstance.add(1, { instance: String(instance), status: String(res.status) });

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

export function handleSummary(data) {
  const metrics = data.metrics || {};
  const reqValues = (metrics.http_reqs && metrics.http_reqs.values) || {};
  const durValues = (metrics.http_req_duration && metrics.http_req_duration.values) || {};
  const checkValues = (metrics.checks && metrics.checks.values) || {};

  const toFixedOrNa = (value) => (typeof value === 'number' ? value.toFixed(2) : 'N/A');

  const totalRequests = typeof reqValues.count === 'number' ? reqValues.count : 0;
  const avgDuration = toFixedOrNa(durValues.avg);
  const p95Duration = toFixedOrNa(durValues['p(95)']);
  const p99Duration = toFixedOrNa(durValues['p(99)']);
  const successRate = typeof checkValues.rate === 'number' ? (checkValues.rate * 100) : null;

  console.log('\n========== 부하 테스트 결과 ==========');
  console.log(`총 요청 수: ${totalRequests}`);
  console.log(`평균 응답시간: ${avgDuration}ms`);
  console.log(`p95 응답시간: ${p95Duration}ms`);
  console.log(`p99 응답시간: ${p99Duration}ms`);
  console.log(`성공률: ${successRate === null ? 'N/A' : `${successRate.toFixed(2)}%`}`);
  console.log('=====================================\n');

  return {};
}
