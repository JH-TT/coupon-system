import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const successCount = new Counter('success_count');
const failCount = new Counter('fail_count');

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
      rate: 5000,              // 초당 2000 요청
      timeUnit: '1s',
      duration: '10s',        // 20초 동안 실행 => 약 50000건
      preAllocatedVUs: 2000,   // 미리 확보할 VU
      maxVUs: 5000,           // 부족하면 최대 2000까지 확장
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
  const userId = __VU; // 각 VU를 고유 유저로 사용

  const payload = JSON.stringify({
    userId: userId,
    couponId: 1,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
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

export function handleSummary(data) {
  const totalRequests = data.metrics.http_reqs.values.count;
  const avgDuration = data.metrics.http_req_duration.values.avg.toFixed(2);
  const p95Duration = data.metrics.http_req_duration.values['p(95)'].toFixed(2);
  const p99Duration = data.metrics.http_req_duration.values['p(99)'].toFixed(2);
  const successRate = data.metrics.checks.values.rate * 100;

  console.log('\n========== 부하 테스트 결과 ==========');
  console.log(`총 요청 수: ${totalRequests}`);
  console.log(`평균 응답시간: ${avgDuration}ms`);
  console.log(`p95 응답시간: ${p95Duration}ms`);
  console.log(`p99 응답시간: ${p99Duration}ms`);
  console.log(`성공률: ${successRate.toFixed(2)}%`);
  console.log('=====================================\n');

  return {
    stdout: JSON.stringify(data, null, 2),
  };
}
