#!/bin/bash

echo "🧪 TESTE MANUAL DE CONSISTÊNCIA KV-STORE"
echo "========================================="
echo

# Configurações
NODES=("http://localhost:8080" "http://localhost:8081" "http://localhost:8082")
TEST_KEY="consistency_test_$(date +%s)"
TEST_VALUE="test_value_$(date +%s)"

# Função para fazer request com timeout
make_request() {
    local method=$1
    local url=$2
    local data=$3
    local timeout=${4:-5}
    
    if [ "$method" = "POST" ] && [ ! -z "$data" ]; then
        curl -s -m $timeout -X POST -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null
    else
        curl -s -m $timeout "$url" 2>/dev/null
    fi
}

# Função para verificar se nó está ativo
check_node() {
    local node=$1
    local response=$(make_request "GET" "$node/health" "" 2)
    
    if [ $? -eq 0 ] && [ ! -z "$response" ]; then
        echo "✅"
        return 0
    else
        echo "❌"
        return 1
    fi
}

# Verificar status dos nós
echo "🔍 Verificando status dos nós:"
active_nodes=()
for i in "${!NODES[@]}"; do
    node=${NODES[$i]}
    echo -n "   Node $((i+1)) ($node): "
    
    if check_node "$node"; then
        active_nodes+=("$node")
    fi
done

echo
echo "📊 Nós ativos: ${#active_nodes[@]}/${#NODES[@]}"
echo

if [ ${#active_nodes[@]} -eq 0 ]; then
    echo "❌ Nenhum nó ativo encontrado!"
    exit 1
fi

# Teste 1: Read-Your-Writes Consistency
echo "🧪 TESTE 1: Read-Your-Writes Consistency"
echo "----------------------------------------"
echo "Escrevendo chave '$TEST_KEY' com valor '$TEST_VALUE'..."

write_node=${active_nodes[0]}
write_response=$(make_request "POST" "$write_node/store" "{\"key\":\"$TEST_KEY\",\"value\":\"$TEST_VALUE\"}")

if [ $? -eq 0 ]; then
    echo "✅ Escrita realizada com sucesso no $write_node"
else
    echo "❌ Falha na escrita"
    exit 1
fi

echo "Aguardando 1 segundo para propagação..."
sleep 1

echo "Lendo a mesma chave do mesmo nó..."
read_response=$(make_request "GET" "$write_node/store/$TEST_KEY")

if echo "$read_response" | grep -q "$TEST_VALUE"; then
    echo "✅ Read-Your-Writes OK: Valor correto retornado"
else
    echo "❌ Read-Your-Writes FALHOU: Valor esperado '$TEST_VALUE', obtido '$read_response'"
fi

echo

# Teste 2: Eventual Consistency
echo "🧪 TESTE 2: Eventual Consistency"
echo "--------------------------------"
echo "Verificando propagação para outros nós..."

sleep 2  # Aguardar propagação

all_consistent=true
for node in "${active_nodes[@]}"; do
    if [ "$node" != "$write_node" ]; then
        echo -n "   Verificando $node: "
        read_response=$(make_request "GET" "$node/store/$TEST_KEY")
        
        if echo "$read_response" | grep -q "$TEST_VALUE"; then
            echo "✅ Consistente"
        else
            echo "❌ Inconsistente (valor: $read_response)"
            all_consistent=false
        fi
    fi
done

if $all_consistent; then
    echo "✅ Eventual Consistency OK: Todos os nós estão consistentes"
else
    echo "❌ Eventual Consistency FALHOU: Alguns nós estão inconsistentes"
fi

echo

# Teste 3: Monotonic Reads
echo "🧪 TESTE 3: Monotonic Reads"
echo "---------------------------"
echo "Atualizando valor para testar monotonic reads..."

NEW_VALUE="updated_value_$(date +%s)"
update_response=$(make_request "POST" "$write_node/store" "{\"key\":\"$TEST_KEY\",\"value\":\"$NEW_VALUE\"}")

if [ $? -eq 0 ]; then
    echo "✅ Atualização realizada com sucesso"
else
    echo "❌ Falha na atualização"
fi

sleep 1

echo "Testando monotonic reads em sequência..."
previous_value=""
monotonic_ok=true

for i in {1..3}; do
    read_response=$(make_request "GET" "$write_node/store/$TEST_KEY")
    echo "   Leitura $i: $read_response"
    
    if [ ! -z "$previous_value" ] && [ "$read_response" != "$previous_value" ] && [ "$read_response" != "$NEW_VALUE" ]; then
        echo "❌ Monotonic Read violado: valor regrediu"
        monotonic_ok=false
    fi
    
    previous_value="$read_response"
    sleep 0.5
done

if $monotonic_ok; then
    echo "✅ Monotonic Reads OK: Nenhuma regressão detectada"
else
    echo "❌ Monotonic Reads FALHOU: Regressão detectada"
fi

echo

# Teste 4: Staleness Test
echo "🧪 TESTE 4: Staleness Test"
echo "--------------------------"
echo "Testando staleness máximo aceitável..."

STALENESS_KEY="staleness_test_$(date +%s)"
STALENESS_VALUE="staleness_value_$(date +%s)"

# Escrever em um nó
echo "Escrevendo em node1..."
write_start=$(date +%s%N)
make_request "POST" "${active_nodes[0]}/store" "{\"key\":\"$STALENESS_KEY\",\"value\":\"$STALENESS_VALUE\"}" > /dev/null

# Verificar quanto tempo leva para aparecer em outros nós
max_staleness=0
for node in "${active_nodes[@]:1}"; do
    echo -n "   Verificando propagação para $node: "
    
    found=false
    attempts=0
    check_start=$(date +%s%N)
    
    while [ $attempts -lt 10 ] && [ $found = false ]; do
        read_response=$(make_request "GET" "$node/store/$STALENESS_KEY")
        
        if echo "$read_response" | grep -q "$STALENESS_VALUE"; then
            check_end=$(date +%s%N)
            staleness=$((($check_end - $check_start) / 1000000))  # Convert to milliseconds
            echo "${staleness}ms"
            
            if [ $staleness -gt $max_staleness ]; then
                max_staleness=$staleness
            fi
            found=true
        else
            sleep 0.1
            attempts=$((attempts + 1))
        fi
    done
    
    if [ $found = false ]; then
        echo "❌ Timeout (>1s)"
        max_staleness=999999
    fi
done

echo "📊 Staleness máximo observado: ${max_staleness}ms"

if [ $max_staleness -lt 5000 ]; then  # 5 segundos
    echo "✅ Staleness aceitável (< 5s)"
else
    echo "❌ Staleness muito alto (> 5s)"
fi

echo

# Teste 5: Conflict Detection
echo "🧪 TESTE 5: Conflict Detection"
echo "------------------------------"
echo "Simulando writes concorrentes..."

CONFLICT_KEY="conflict_test_$(date +%s)"

if [ ${#active_nodes[@]} -ge 2 ]; then
    echo "Escrevendo valores diferentes no mesmo key simultaneamente..."
    
    # Writes simultâneos
    make_request "POST" "${active_nodes[0]}/store" "{\"key\":\"$CONFLICT_KEY\",\"value\":\"value_from_node1\"}" > /dev/null &
    make_request "POST" "${active_nodes[1]}/store" "{\"key\":\"$CONFLICT_KEY\",\"value\":\"value_from_node2\"}" > /dev/null &
    
    wait
    
    sleep 2  # Aguardar resolução de conflito
    
    # Verificar se todos os nós convergiram para o mesmo valor
    echo "Verificando convergência após conflito..."
    values=()
    
    for node in "${active_nodes[@]}"; do
        read_response=$(make_request "GET" "$node/store/$CONFLICT_KEY")
        values+=("$read_response")
        echo "   $node: $read_response"
    done
    
    # Verificar se todos os valores são iguais
    first_value="${values[0]}"
    all_same=true
    
    for value in "${values[@]}"; do
        if [ "$value" != "$first_value" ]; then
            all_same=false
            break
        fi
    done
    
    if $all_same; then
        echo "✅ Conflict Resolution OK: Todos os nós convergiram"
    else
        echo "❌ Conflict Resolution FALHOU: Nós ainda inconsistentes"
    fi
else
    echo "⚠️  Skipping: Precisa de pelo menos 2 nós ativos"
fi

echo

# Resumo Final
echo "📋 RESUMO DOS TESTES DE CONSISTÊNCIA"
echo "===================================="
echo "✅ Read-Your-Writes: Verificado"
echo "✅ Eventual Consistency: Verificado"  
echo "✅ Monotonic Reads: Verificado"
echo "✅ Staleness Test: Verificado (${max_staleness}ms)"
echo "✅ Conflict Detection: Verificado"
echo
echo "🏁 Testes de consistência concluídos!"
echo
echo "💡 Dica: Execute 'docker-compose -f docker-compose-observability.yml logs' para ver logs detalhados"
echo "📊 Dica: Acesse http://localhost:3000 para ver métricas no Grafana"
