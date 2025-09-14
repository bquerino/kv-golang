import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

// Métricas customizadas para testes de consistência
let eventualConsistencyViolations = new Counter('eventual_consistency_violations');
let convergenceTime = new Trend('convergence_time_ms');
let readYourWritesViolations = new Counter('read_your_writes_violations');
let causalConsistencyViolations = new Counter('causal_consistency_violations');

export let options = {
  stages: [
    { duration: '1m', target: 5 },    // Warm up
    { duration: '3m', target: 20 },   // Teste de consistência
    { duration: '2m', target: 30 },   // Pico de stress
    { duration: '1m', target: 0 },    // Cool down
  ],
  thresholds: {
    http_req_duration: ['p(95)<1000'],
    http_req_failed: ['rate<0.1'],
    eventual_consistency_violations: ['count<5'],
    convergence_time_ms: ['p(95)<10000'],
    read_your_writes_violations: ['count<3'],
    causal_consistency_violations: ['count<2'],
  },
};

const BASE_URL = 'http://nginx:8080';
const NODES = [
  'http://node1:8081',
  'http://node2:8082', 
  'http://node3:8083'
];

// Armazena histórico de escritas para testes de consistência
let writeHistory = {};
let causalHistory = {};

export default function () {
  const userId = __VU;
  const iteration = __ITER;
  const testId = `user_${userId}_${iteration}`;
  
  // Seleciona tipo de teste baseado na probabilidade
  const testType = Math.random();
  
  if (testType < 0.4) {
    // 40% - Teste de Consistência Eventual
    testEventualConsistency(testId);
  } else if (testType < 0.7) {
    // 30% - Teste Read-Your-Writes
    testReadYourWrites(testId);
  } else if (testType < 0.9) {
    // 20% - Teste de Consistência Causal
    testCausalConsistency(testId, userId);
  } else {
    // 10% - Teste de Convergência
    testConvergence(testId);
  }
  
  sleep(Math.random() * 1 + 0.5); // Sleep entre 0.5-1.5s
}

function testEventualConsistency(testId) {
  const key = `eventual_${testId}`;
  const value = `value_${Math.random().toString(36).substr(2, 9)}`;
  
  // Escreve em um nó específico
  const writeNode = NODES[Math.floor(Math.random() * NODES.length)];
  const writeResponse = http.post(`${writeNode}/put`, JSON.stringify({
    key: key,
    value: value
  }), {
    headers: { 'Content-Type': 'application/json' }
  });
  
  if (writeResponse.status === 200) {
    const writeTime = Date.now();
    writeHistory[key] = { value, writeTime, node: writeNode };
    
    // Aguarda um pouco e lê de todos os nós
    sleep(2);
    
    let consistentReads = 0;
    let totalReads = 0;
    
    NODES.forEach(node => {
      const readResponse = http.get(`${node}/get?key=${key}`);
      if (readResponse.status === 200) {
        totalReads++;
        try {
          const data = JSON.parse(readResponse.body);
          if (data.value === value) {
            consistentReads++;
          }
        } catch (e) {
          console.log(`Error parsing response from ${node}: ${e}`);
        }
      }
    });
    
    // Verifica consistência eventual
    if (totalReads > 0 && consistentReads < totalReads) {
      eventualConsistencyViolations.add(1);
      console.log(`Eventual consistency violation for key ${key}: ${consistentReads}/${totalReads} nodes consistent`);
    }
  }
}

function testReadYourWrites(testId) {
  const key = `ryw_${testId}`;
  const value = `value_${Math.random().toString(36).substr(2, 9)}`;
  
  // Escreve via load balancer
  const writeResponse = http.post(`${BASE_URL}/put`, JSON.stringify({
    key: key,
    value: value
  }), {
    headers: { 'Content-Type': 'application/json' }
  });
  
  if (writeResponse.status === 200) {
    // Imediatamente tenta ler o que escreveu
    sleep(0.1);
    
    const readResponse = http.get(`${BASE_URL}/get?key=${key}`);
    if (readResponse.status === 200) {
      try {
        const data = JSON.parse(readResponse.body);
        if (data.value !== value) {
          readYourWritesViolations.add(1);
          console.log(`Read-your-writes violation: wrote ${value}, read ${data.value}`);
        }
      } catch (e) {
        console.log(`Error parsing read response: ${e}`);
      }
    }
  }
}

function testCausalConsistency(testId, userId) {
  // Teste de consistência causal: se A causou B, então B deve ser visível onde A é visível
  const keyA = `causal_a_${testId}`;
  const keyB = `causal_b_${testId}`;
  const valueA = `valueA_${Math.random().toString(36).substr(2, 9)}`;
  const valueB = `valueB_${Math.random().toString(36).substr(2, 9)}`;
  
  // Escreve A
  const writeAResponse = http.post(`${BASE_URL}/put`, JSON.stringify({
    key: keyA,
    value: valueA
  }), {
    headers: { 'Content-Type': 'application/json' }
  });
  
  if (writeAResponse.status === 200) {
    // Aguarda A se propagar
    sleep(1);
    
    // Escreve B (que depende causalmente de A)
    const writeBResponse = http.post(`${BASE_URL}/put`, JSON.stringify({
      key: keyB,
      value: valueB,
      causedBy: keyA // Metadado indicando dependência causal
    }), {
      headers: { 'Content-Type': 'application/json' }
    });
    
    if (writeBResponse.status === 200) {
      sleep(2);
      
      // Verifica se em todos os nós onde B é visível, A também é visível
      NODES.forEach(node => {
        const readBResponse = http.get(`${node}/get?key=${keyB}`);
        if (readBResponse.status === 200) {
          try {
            const dataB = JSON.parse(readBResponse.body);
            if (dataB.value === valueB) {
              // B é visível, A também deve ser visível
              const readAResponse = http.get(`${node}/get?key=${keyA}`);
              if (readAResponse.status === 200) {
                const dataA = JSON.parse(readAResponse.body);
                if (dataA.value !== valueA) {
                  causalConsistencyViolations.add(1);
                  console.log(`Causal consistency violation: B visible but not A on ${node}`);
                }
              } else {
                causalConsistencyViolations.add(1);
                console.log(`Causal consistency violation: B visible but A not found on ${node}`);
              }
            }
          } catch (e) {
            console.log(`Error in causal consistency test: ${e}`);
          }
        }
      });
    }
  }
}

function testConvergence(testId) {
  const key = `convergence_${testId}`;
  const value = `value_${Math.random().toString(36).substr(2, 9)}`;
  
  const startTime = Date.now();
  
  // Escreve o valor
  const writeResponse = http.post(`${BASE_URL}/put`, JSON.stringify({
    key: key,
    value: value
  }), {
    headers: { 'Content-Type': 'application/json' }
  });
  
  if (writeResponse.status === 200) {
    let converged = false;
    let attempts = 0;
    const maxAttempts = 30; // 30 segundos máximo
    
    while (!converged && attempts < maxAttempts) {
      sleep(1);
      attempts++;
      
      let consistentNodes = 0;
      let totalNodes = 0;
      
      // Verifica todos os nós
      NODES.forEach(node => {
        const readResponse = http.get(`${node}/get?key=${key}`);
        if (readResponse.status === 200) {
          totalNodes++;
          try {
            const data = JSON.parse(readResponse.body);
            if (data.value === value) {
              consistentNodes++;
            }
          } catch (e) {
            console.log(`Error reading from ${node}: ${e}`);
          }
        }
      });
      
      if (totalNodes > 0 && consistentNodes === totalNodes) {
        converged = true;
        const convergenceTimeMs = Date.now() - startTime;
        convergenceTime.add(convergenceTimeMs);
        console.log(`Convergence achieved for ${key} in ${convergenceTimeMs}ms`);
      }
    }
    
    if (!converged) {
      console.log(`Convergence timeout for key ${key} after ${attempts} attempts`);
    }
  }
}

export function handleSummary(data) {
  const summary = {
    testType: "Leaderless Consistency Test",
    duration: `${data.state.testRunDurationMs}ms`,
    vus: data.metrics.vus?.values?.max || 0,
    iterations: data.metrics.iterations?.values?.count || 0,
    
    httpMetrics: {
      requests: data.metrics.http_reqs?.values?.count || 0,
      failed: `${((data.metrics.http_req_failed?.values?.rate || 0) * 100).toFixed(2)}%`,
      avgDuration: `${(data.metrics.http_req_duration?.values?.avg || 0).toFixed(2)}ms`,
      p95Duration: `${(data.metrics.http_req_duration?.values?.['p(95)'] || 0).toFixed(2)}ms`
    },
    
    consistencyMetrics: {
      eventualConsistencyViolations: data.metrics.eventual_consistency_violations?.values?.count || 0,
      readYourWritesViolations: data.metrics.read_your_writes_violations?.values?.count || 0,
      causalConsistencyViolations: data.metrics.causal_consistency_violations?.values?.count || 0,
      avgConvergenceTime: `${(data.metrics.convergence_time_ms?.values?.avg || 0).toFixed(2)}ms`,
      p95ConvergenceTime: `${(data.metrics.convergence_time_ms?.values?.['p(95)'] || 0).toFixed(2)}ms`
    }
  };

  const textOutput = `
=== LEADERLESS CONSISTENCY TEST RESULTS ===
Test Duration: ${summary.duration}
Max VUs: ${summary.vus}
Total Iterations: ${summary.iterations}

HTTP Performance:
- Total Requests: ${summary.httpMetrics.requests}
- Failed Requests: ${summary.httpMetrics.failed}
- Avg Response Time: ${summary.httpMetrics.avgDuration}
- P95 Response Time: ${summary.httpMetrics.p95Duration}

Consistency Analysis:
- Eventual Consistency Violations: ${summary.consistencyMetrics.eventualConsistencyViolations}
- Read-Your-Writes Violations: ${summary.consistencyMetrics.readYourWritesViolations}
- Causal Consistency Violations: ${summary.consistencyMetrics.causalConsistencyViolations}
- Avg Convergence Time: ${summary.consistencyMetrics.avgConvergenceTime}
- P95 Convergence Time: ${summary.consistencyMetrics.p95ConvergenceTime}

${summary.consistencyMetrics.eventualConsistencyViolations === 0 ? 
  '✅ EVENTUAL CONSISTENCY: PASSED' : 
  '❌ EVENTUAL CONSISTENCY: VIOLATIONS DETECTED'}

${summary.consistencyMetrics.readYourWritesViolations === 0 ? 
  '✅ READ-YOUR-WRITES: PASSED' : 
  '❌ READ-YOUR-WRITES: VIOLATIONS DETECTED'}

${summary.consistencyMetrics.causalConsistencyViolations === 0 ? 
  '✅ CAUSAL CONSISTENCY: PASSED' : 
  '❌ CAUSAL CONSISTENCY: VIOLATIONS DETECTED'}
`;

  return {
    'stdout': textOutput,
    'results.json': JSON.stringify(summary, null, 2),
  };
}
