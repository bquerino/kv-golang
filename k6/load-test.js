import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

// Custom metrics for consistency checks
let readYourWritesViolations = new Counter('read_your_writes_violations');
let monotonicReadViolations = new Counter('monotonic_read_violations');
let stalenessTime = new Trend('staleness_time');
let convergenceTime = new Trend('convergence_time');

// Configuration
export let options = {
  stages: [
    { duration: '2m', target: 10 },   // Ramp up
    { duration: '5m', target: 50 },   // Stay at 50 users
    { duration: '2m', target: 100 },  // Ramp to 100 users
    { duration: '5m', target: 100 },  // Stay at 100 users
    { duration: '2m', target: 0 },    // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% of requests must be below 500ms
    http_req_failed: ['rate<0.05'],    // Error rate must be below 5%
    read_your_writes_violations: ['count<10'], // Max 10 violations
    staleness_time: ['p(95)<5000'],    // 95% of reads should be stale <5s
  },
};

const BASE_URL = 'http://nginx:8080';
const NODES = ['http://node1:8081', 'http://node2:8082', 'http://node3:8083'];

// Store write timestamps and values for consistency checks
let writeHistory = {};

export default function () {
  const testId = `user_${__VU}_${__ITER}`;
  const key = `key_${testId}`;
  const value = `value_${Math.random()}`;
  
  // Test scenario selection
  const scenario = Math.random();
  
  if (scenario < 0.6) {
    // 60% - Standard PUT/GET test
    testStandardOperations(key, value);
  } else if (scenario < 0.8) {
    // 20% - Consistency tests
    testConsistency(key, value);
  } else {
    // 20% - Convergence tests
    testConvergence(key, value);
  }
  
  sleep(Math.random() * 2); // Random sleep 0-2s
}

function testStandardOperations(key, value) {
  // PUT operation
  const putResponse = http.post(`${BASE_URL}/put`, {
    key: key,
    value: value
  });
  
  check(putResponse, {
    'PUT status is 200': (r) => r.status === 200,
    'PUT response time < 500ms': (r) => r.timings.duration < 500,
  });
  
  if (putResponse.status === 200) {
    writeHistory[key] = {
      value: value,
      timestamp: Date.now(),
      writtenBy: __VU
    };
  }
  
  sleep(0.1);
  
  // GET operation
  const getResponse = http.get(`${BASE_URL}/get?key=${key}`);
  
  check(getResponse, {
    'GET status is 200': (r) => r.status === 200,
    'GET response time < 200ms': (r) => r.timings.duration < 200,
  });
}

function testConsistency(key, value) {
  // Write to leader
  const putResponse = http.post(`${BASE_URL}/put`, {
    key: key,
    value: value
  });
  
  if (putResponse.status === 200) {
    const writeTime = Date.now();
    writeHistory[key] = {
      value: value,
      timestamp: writeTime,
      writtenBy: __VU
    };
    
    // Test read-your-writes consistency
    sleep(0.05); // Small delay
    
    const readResponse = http.get(`${BASE_URL}/get?key=${key}`);
    if (readResponse.status === 200) {
      const readValue = JSON.parse(readResponse.body).value;
      
      if (readValue !== value) {
        readYourWritesViolations.add(1);
        console.log(`Read-your-writes violation: wrote ${value}, read ${readValue}`);
      }
      
      // Calculate staleness
      const staleness = Date.now() - writeTime;
      stalenessTime.add(staleness);
    }
    
    // Test monotonic reads - read from multiple nodes
    testMonotonicReads(key);
  }
}

function testMonotonicReads(key) {
  let lastVersion = null;
  let lastValue = null;
  
  for (let node of NODES) {
    const response = http.get(`${node}/get?key=${key}`);
    if (response.status === 200) {
      const data = JSON.parse(response.body);
      
      if (lastVersion !== null) {
        // Check if we're going backwards in time
        if (data.vectorClock && isOlderVersion(data.vectorClock, lastVersion)) {
          monotonicReadViolations.add(1);
          console.log(`Monotonic read violation on node ${node}`);
        }
      }
      
      lastVersion = data.vectorClock;
      lastValue = data.value;
    }
    
    sleep(0.01);
  }
}

function testConvergence(key, value) {
  const convergenceStart = Date.now();
  
  // Write to cluster
  const putResponse = http.post(`${BASE_URL}/put`, {
    key: key,
    value: value
  });
  
  if (putResponse.status === 200) {
    // Wait for convergence by reading from all nodes
    let converged = false;
    let attempts = 0;
    const maxAttempts = 20;
    
    while (!converged && attempts < maxAttempts) {
      let consistentReads = 0;
      
      for (let node of NODES) {
        const response = http.get(`${node}/get?key=${key}`);
        if (response.status === 200) {
          const data = JSON.parse(response.body);
          if (data.value === value) {
            consistentReads++;
          }
        }
      }
      
      if (consistentReads === NODES.length) {
        converged = true;
        const convergenceTimeTaken = Date.now() - convergenceStart;
        convergenceTime.add(convergenceTimeTaken);
      }
      
      attempts++;
      sleep(0.1);
    }
    
    if (!converged) {
      console.log(`Convergence timeout for key ${key}`);
    }
  }
}

function isOlderVersion(vc1, vc2) {
  // Simplified vector clock comparison
  // In real implementation, this should be more sophisticated
  if (!vc1 || !vc2) return false;
  
  try {
    const clock1 = JSON.parse(vc1);
    const clock2 = JSON.parse(vc2);
    
    for (let node in clock1) {
      if (clock1[node] > (clock2[node] || 0)) {
        return false;
      }
    }
    
    return true;
  } catch (e) {
    return false;
  }
}

export function handleSummary(data) {
  return {
    'stdout': textSummary(data, { indent: ' ', enableColors: true }),
    'results.json': JSON.stringify(data, null, 2),
    'results.html': htmlReport(data),
  };
}

function textSummary(data, options) {
  return `
=== K6 Load Test Results ===
Duration: ${data.state.testRunDurationMs}ms
VUs: ${data.metrics.vus.values.max}

Requests:
- Total: ${data.metrics.http_reqs.values.count}
- Failed: ${data.metrics.http_req_failed.values.rate * 100}%
- RPS: ${data.metrics.http_reqs.values.rate}

Latency:
- Avg: ${data.metrics.http_req_duration.values.avg}ms
- P95: ${data.metrics.http_req_duration.values['p(95)']}ms
- P99: ${data.metrics.http_req_duration.values['p(99)']}ms

Consistency:
- Read-your-writes violations: ${data.metrics.read_your_writes_violations?.values?.count || 0}
- Monotonic read violations: ${data.metrics.monotonic_read_violations?.values?.count || 0}
- Staleness P95: ${data.metrics.staleness_time?.values?.['p(95)'] || 0}ms
- Convergence P95: ${data.metrics.convergence_time?.values?.['p(95)'] || 0}ms
`;
}

function htmlReport(data) {
  return `<!DOCTYPE html>
<html>
<head><title>K6 Test Results</title></head>
<body>
<h1>KV-Store Load Test Results</h1>
<pre>${textSummary(data)}</pre>
</body>
</html>`;
}
