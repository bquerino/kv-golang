#!/bin/bash

echo "🧪 VALIDAÇÃO COMPLETA - TODOS OS MODOS COM OBSERVABILIDADE"
echo "=========================================================="
echo

# Cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para testar um compose file
test_compose_file() {
    local file=$1
    local mode_name=$2
    
    echo -e "${BLUE}📋 Testando: $mode_name${NC}"
    echo -e "${BLUE}📁 Arquivo: $file${NC}"
    echo
    
    # Limpar ambiente
    echo "🧹 Limpando ambiente anterior..."
    docker-compose -f "$file" down -v > /dev/null 2>&1
    
    # Iniciar serviços
    echo "🚀 Iniciando serviços..."
    docker-compose -f "$file" up --build -d
    
    # Aguardar serviços
    echo "⏳ Aguardando serviços ficarem disponíveis..."
    sleep 30
    
    # Verificar serviços básicos
    echo "🔍 Verificando serviços..."
    
    local all_good=true
    
    # Verificar KV-Store
    if curl -s http://localhost:8080/health > /dev/null; then
        echo -e "${GREEN}✅ KV-Store (8080): OK${NC}"
    else
        echo -e "${RED}❌ KV-Store (8080): FAIL${NC}"
        all_good=false
    fi
    
    # Verificar Prometheus
    if curl -s http://localhost:9090/api/v1/targets > /dev/null; then
        echo -e "${GREEN}✅ Prometheus (9090): OK${NC}"
    else
        echo -e "${RED}❌ Prometheus (9090): FAIL${NC}"
        all_good=false
    fi
    
    # Verificar Grafana
    if curl -s http://localhost:3000/api/health > /dev/null; then
        echo -e "${GREEN}✅ Grafana (3000): OK${NC}"
    else
        echo -e "${RED}❌ Grafana (3000): FAIL${NC}"
        all_good=false
    fi
    
    # Verificar métricas dos nós
    for port in 9091 9092 9093; do
        if curl -s "http://localhost:$port/metrics" | grep -q "kvstore_"; then
            echo -e "${GREEN}✅ Node Metrics ($port): OK${NC}"
        else
            echo -e "${RED}❌ Node Metrics ($port): FAIL${NC}"
            all_good=false
        fi
    done
    
    # Teste básico PUT/GET
    echo "🧪 Testando operações básicas..."
    
    test_key="test_$(date +%s)"
    test_value="value_$(date +%s)"
    
    # PUT
    put_response=$(curl -s -X POST http://localhost:8080/store \
        -H "Content-Type: application/json" \
        -d "{\"key\":\"$test_key\",\"value\":\"$test_value\"}")
    
    if echo "$put_response" | grep -q "PUT_ACK"; then
        echo -e "${GREEN}✅ PUT operation: OK${NC}"
    else
        echo -e "${RED}❌ PUT operation: FAIL${NC}"
        echo "Response: $put_response"
        all_good=false
    fi
    
    # GET
    sleep 2  # Aguardar replicação
    get_response=$(curl -s "http://localhost:8080/store/$test_key")
    
    if echo "$get_response" | grep -q "$test_value"; then
        echo -e "${GREEN}✅ GET operation: OK${NC}"
    else
        echo -e "${RED}❌ GET operation: FAIL${NC}"
        echo "Response: $get_response"
        all_good=false
    fi
    
    # Verificar Prometheus targets
    echo "📊 Verificando targets Prometheus..."
    targets_response=$(curl -s http://localhost:9090/api/v1/targets | jq -r '.data.activeTargets[].health' 2>/dev/null)
    
    if echo "$targets_response" | grep -q "up"; then
        echo -e "${GREEN}✅ Prometheus targets: OK${NC}"
    else
        echo -e "${YELLOW}⚠️  Prometheus targets: Checking...${NC}"
    fi
    
    # Resultado final do teste
    if $all_good; then
        echo -e "${GREEN}🎉 TESTE COMPLETO: $mode_name - SUCESSO${NC}"
    else
        echo -e "${RED}💥 TESTE COMPLETO: $mode_name - FALHOU${NC}"
    fi
    
    echo
    echo "📋 Status dos serviços:"
    docker-compose -f "$file" ps
    echo
    
    # Parar serviços
    echo "🛑 Parando serviços..."
    docker-compose -f "$file" down > /dev/null 2>&1
    
    echo -e "${BLUE}═══════════════════════════════════════════════${NC}"
    echo
    
    return $([ "$all_good" = true ] && echo 0 || echo 1)
}

# Configurar dependências
echo "📦 Configurando dependências..."
go mod tidy

echo
echo "🚀 INICIANDO VALIDAÇÃO DE TODOS OS MODOS"
echo

# Array para rastrear resultados
declare -a results

# Teste 1: Modo LEADERLESS (padrão)
test_compose_file "docker-compose.yml" "LEADERLESS (padrão)"
results[0]=$?

# Teste 2: Modo LEADERLESS (explícito)
test_compose_file "docker-compose-leaderless.yml" "LEADERLESS (explícito)"
results[1]=$?

# Teste 3: Modo LEADER-FOLLOWER
test_compose_file "docker-compose-leader-follower.yml" "LEADER-FOLLOWER"
results[2]=$?

# Teste 4: Desenvolvimento (Observability Only)
test_compose_file "docker-compose-observability.yml" "OBSERVABILITY ONLY"
results[3]=$?

# Resumo final
echo -e "${BLUE}📋 RESUMO FINAL DOS TESTES${NC}"
echo "═══════════════════════════════════════"

modes=("LEADERLESS (padrão)" "LEADERLESS (explícito)" "LEADER-FOLLOWER" "OBSERVABILITY ONLY")
files=("docker-compose.yml" "docker-compose-leaderless.yml" "docker-compose-leader-follower.yml" "docker-compose-observability.yml")

total_success=0
total_tests=4

for i in {0..3}; do
    if [ ${results[$i]} -eq 0 ]; then
        echo -e "${GREEN}✅ ${modes[$i]}: SUCESSO${NC}"
        total_success=$((total_success + 1))
    else
        echo -e "${RED}❌ ${modes[$i]}: FALHOU${NC}"
    fi
done

echo
echo -e "${BLUE}📊 ESTATÍSTICAS FINAIS:${NC}"
echo "   Total de testes: $total_tests"
echo "   Sucessos: $total_success"
echo "   Falhas: $((total_tests - total_success))"

if [ $total_success -eq $total_tests ]; then
    echo -e "${GREEN}🎉 TODOS OS TESTES PASSARAM! 🎉${NC}"
    echo "✅ Observabilidade implementada com sucesso em todos os modos!"
else
    echo -e "${RED}💥 ALGUNS TESTES FALHARAM${NC}"
    echo "⚠️  Verifique os logs acima para detalhes dos problemas"
fi

echo
echo -e "${BLUE}💡 PRÓXIMOS PASSOS:${NC}"
echo "1. Escolha um modo para trabalhar:"
echo "   ./scripts/run-complete-stack.sh"
echo
echo "2. Acesse os dashboards:"
echo "   - Grafana: http://localhost:3000 (admin/admin)"
echo "   - Prometheus: http://localhost:9090"
echo
echo "3. Execute testes específicos:"
echo "   - Load test: docker-compose --profile testing run --rm k6 run /scripts/load-test.js"
echo "   - Failover test: docker-compose --profile testing run --rm k6 run /scripts/failover-test.js"
echo "   - Consistency test: ./scripts/test-consistency-checks.sh"

echo
echo "🏁 Validação completa finalizada!"
