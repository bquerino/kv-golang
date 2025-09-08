#!/bin/bash

echo "📦 Configurando dependências para observabilidade..."
echo

echo "🔄 Adicionando dependências Prometheus..."
go mod tidy

echo "📊 Criando configuração Prometheus..."
cat > prometheus.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "alert_rules.yml"

scrape_configs:
  - job_name: 'kv-store'
    static_configs:
      - targets: ['localhost:8081', 'localhost:8082', 'localhost:8083']
    metrics_path: '/metrics'
    scrape_interval: 5s
    
  - job_name: 'kv-store-nginx'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 10s

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
EOF

echo "🚨 Criando regras de alerta..."
cat > alert_rules.yml << 'EOF'
groups:
- name: kv-store
  rules:
  - alert: HighRequestLatency
    expr: histogram_quantile(0.95, kvstore_request_duration_seconds_bucket) > 1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High request latency detected"
      description: "95th percentile latency is {{ $value }}s"

  - alert: ReplicationFailure
    expr: rate(kvstore_replication_errors_total[5m]) > 0.1
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "High replication failure rate"
      description: "Replication error rate is {{ $value }}/s"

  - alert: NoLeader
    expr: sum(kvstore_is_leader) == 0
    for: 30s
    labels:
      severity: critical
    annotations:
      summary: "No leader in cluster"
      description: "No node is currently the leader"

  - alert: ConsistencyViolation
    expr: rate(kvstore_read_your_writes_violations_total[5m]) > 0
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "Consistency violations detected"
      description: "Read-your-writes violations: {{ $value }}/s"
EOF

echo "📊 Criando configuração Grafana..."
mkdir -p grafana/provisioning/{datasources,dashboards}

cat > grafana/provisioning/datasources/prometheus.yml << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOF

cat > grafana/provisioning/dashboards/dashboard.yml << 'EOF'
apiVersion: 1

providers:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
EOF

echo "✅ Configuração de observabilidade criada!"
echo "💡 Execute './setup-observability.sh' para inicializar dependências"
