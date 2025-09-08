import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Custom metrics for failover scenarios
let leaderElectionTime = new Trend('leader_election_time');
let dataLossRate = new Rate('data_loss_rate');
let availabilityRate = new Rate('availability_rate');

export let options = {
  scenarios: {
    normal_operations: {
      executor: 'constant-vus',
      vus: 20,
      duration: '2m',
      startTime: '0s',
    },
    failover_test: {
      executor: 'constant-vus', 
      vus: 5,
      duration: '5m',
      startTime: '1m',
    },
    recovery_test: {
      executor: 'constant-vus',
      vus: 10, 
      duration: '2m',
      startTime: '4m',
    }
  },
  thresholds: {
    leader_election_time: ['p(95)<10000'], // Election should complete in <10s
    data_loss_rate: ['rate<0.01'],         // <1% data loss
    availability_rate: ['rate>0.99'],      // >99% availability
  },
};

const BASE_URL = 'http://nginx:8080';
const NODES = ['http://node1:8081', 'http://node2:8082', 'http://node3:8083'];

// Global state for tracking operations
let operationHistory = new Map();
let currentLeader = null;

export default function () {
  const scenario = __ENV.K6_SCENARIO_NAME;
  
  switch (scenario) {
    case 'normal_operations':
      runNormalOperations();
      break;
    case 'failover_test':
      runFailoverTest();
      break;
    case 'recovery_test':
      runRecoveryTest();
      break;
    default:
      runNormalOperations();
  }
}

function runNormalOperations() {
  const key = `normal_${__VU}_${__ITER}`;
  const value = `value_${Date.now()}`;
  
  // Standard PUT/GET operations
  const putResponse = http.post(`${BASE_URL}/put`, { key, value });
  const putSuccess = check(putResponse, {
    'PUT successful': (r) => r.status === 200,
  });
  
  availabilityRate.add(putSuccess);
  
  if (putSuccess) {
    operationHistory.set(key, { value, timestamp: Date.now() });
    
    sleep(0.1);
    
    const getResponse = http.get(`${BASE_URL}/get?key=${key}`);
    const getSuccess = check(getResponse, {
      'GET successful': (r) => r.status === 200,
      'GET correct value': (r) => r.status === 200 && JSON.parse(r.body).value === value,
    });
    
    availabilityRate.add(getSuccess);
  }
  
  sleep(1);
}

function runFailoverTest() {
  // This scenario tests behavior during leader failures
  if (__ITER === 0) {
    // First iteration: identify current leader
    identifyCurrentLeader();
  }
  
  if (__ITER === 5 && __VU === 1) {
    // Simulate leader failure (would be done externally in real test)
    console.log('=== SIMULATING LEADER FAILURE ===');
    // In practice, this would call Docker API or send shutdown command
  }
  
  const key = `failover_${__VU}_${__ITER}`;
  const value = `value_${Date.now()}`;
  
  const operationStart = Date.now();
  
  // Try to perform operations during failover
  const putResponse = http.post(`${BASE_URL}/put`, { key, value });
  
  if (putResponse.status === 200) {
    operationHistory.set(key, { value, timestamp: Date.now() });
    availabilityRate.add(true);
  } else {
    availabilityRate.add(false);
    console.log(`PUT failed during failover: ${putResponse.status}`);
  }
  
  // Check if we can still read existing data
  if (operationHistory.size > 0) {
    const randomKey = Array.from(operationHistory.keys())[Math.floor(Math.random() * operationHistory.size)];
    const expectedValue = operationHistory.get(randomKey).value;
    
    const getResponse = http.get(`${BASE_URL}/get?key=${randomKey}`);
    if (getResponse.status === 200) {
      const actualValue = JSON.parse(getResponse.body).value;
      if (actualValue !== expectedValue) {
        dataLossRate.add(true);
        console.log(`Data loss detected: key=${randomKey}, expected=${expectedValue}, actual=${actualValue}`);
      } else {
        dataLossRate.add(false);
      }
    }
  }
  
  sleep(2);
}

function runRecoveryTest() {
  // Test behavior after recovery
  const key = `recovery_${__VU}_${__ITER}`;
  const value = `value_${Date.now()}`;
  
  // Verify system is back to normal operation
  const putResponse = http.post(`${BASE_URL}/put`, { key, value });
  const putSuccess = check(putResponse, {
    'Recovery PUT successful': (r) => r.status === 200,
  });
  
  if (putSuccess) {
    operationHistory.set(key, { value, timestamp: Date.now() });
    
    // Verify data consistency across nodes
    sleep(0.5); // Allow replication
    
    let consistentNodes = 0;
    for (let node of NODES) {
      const getResponse = http.get(`${node}/get?key=${key}`);
      if (getResponse.status === 200) {
        const nodeValue = JSON.parse(getResponse.body).value;
        if (nodeValue === value) {
          consistentNodes++;
        }
      }
    }
    
    const isConsistent = consistentNodes === NODES.length;
    check(null, {
      'Data consistent across nodes': () => isConsistent,
    });
    
    if (!isConsistent) {
      console.log(`Consistency issue: ${consistentNodes}/${NODES.length} nodes have correct value`);
    }
  }
  
  availabilityRate.add(putSuccess);
  sleep(1);
}

function identifyCurrentLeader() {
  for (let node of NODES) {
    const statusResponse = http.get(`${node}/status`);
    if (statusResponse.status === 200) {
      const status = statusResponse.body;
      if (status.includes('is_leader:true')) {
        currentLeader = node;
        console.log(`Current leader identified: ${node}`);
        break;
      }
    }
  }
}

export function handleSummary(data) {
  const summary = {
    'stdout': createTextSummary(data),
    'failover-results.json': JSON.stringify({
      timestamp: new Date().toISOString(),
      scenarios: data.metrics,
      custom_metrics: {
        leader_election_time: data.metrics.leader_election_time?.values,
        data_loss_rate: data.metrics.data_loss_rate?.values,
        availability_rate: data.metrics.availability_rate?.values,
      }
    }, null, 2),
  };
  
  return summary;
}

function createTextSummary(data) {
  return `
=== Failover Test Results ===

Availability: ${(data.metrics.availability_rate?.values?.rate || 0) * 100}%
Data Loss Rate: ${(data.metrics.data_loss_rate?.values?.rate || 0) * 100}%

Leader Election:
- P95 Time: ${data.metrics.leader_election_time?.values?.['p(95)'] || 'N/A'}ms
- Max Time: ${data.metrics.leader_election_time?.values?.max || 'N/A'}ms

HTTP Metrics:
- Total Requests: ${data.metrics.http_reqs?.values?.count || 0}
- Error Rate: ${(data.metrics.http_req_failed?.values?.rate || 0) * 100}%
- P95 Latency: ${data.metrics.http_req_duration?.values?.['p(95)'] || 'N/A'}ms

Operations History: ${operationHistory.size} operations tracked
`;
}
