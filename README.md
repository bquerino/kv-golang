# KV-Store Distribuído com Gossip Protocol

Este projeto implementa um KV-Store (Key-Value Store) distribuído usando Go, com suporte para **Gossip Protocol** e **persistência em disco**. Ele foi projetado para funcionar em um ambiente distribuído com múltiplos nós que se comunicam entre si.

## Funcionalidades

- **Gossip Protocol**: Comunicação entre nós distribuídos para propagação de chaves e valores.
- **Persistência em disco**: Chaves e valores são salvos em arquivos locais, garantindo que os dados sejam recuperados após reiniciar o sistema.
- **Vector Clocks**: Controle de versões para garantir a consistência dos dados em ambientes distribuídos.
- **Resolução de Conflitos**: Quando há conflitos entre versões de dados, os valores são mesclados.

## Requisitos

- [Go](https://golang.org/dl/) (v1.16 ou superior)
- [Docker](https://www.docker.com/) e Docker Compose (para execução via contêineres)
- Um ambiente que permita múltiplas instâncias rodando (múltiplos terminais ou servidores).

## Execução com Docker

É possível levantar três nós da aplicação e um balanceador de carga Nginx usando o Docker Compose incluso neste repositório:

```bash
docker-compose up --build -d
```

O Nginx ficará exposto na porta **8080**, encaminhando o tráfego para os três nós. Para interagir com o cluster, utilize o cliente apontando para o load balancer:


```bash
go run ./cmd/client/main.go localhost 8080
```

Assim, qualquer comando enviado será roteado para um dos nós do cluster automaticamente.

## Como Testar o Projeto sem Docker

### 1. Clonar o Repositório

Clone o repositório para a sua máquina:

```bash
git clone https://github.com/bquerino/kv-golang.git
cd kv-golang
```

### 2. Executar Múltiplos Nós

Para simular um ambiente distribuído com múltiplos nós, utilize o modo servidor para cada instância:

**Terminal 1: Rodar o Nó 1 (servidor)**


```bash
go run ./cmd/server/main.go node1 8081
```

**Terminal 2: Rodar o Nó 2 (servidor)**


```bash
go run ./cmd/server/main.go node2 8082
```

**Terminal 3: Rodar o Nó 3 (servidor)**


```bash
go run ./cmd/server/main.go node3 8083
```

### 3. Usar o Cliente Interativo

Abra um terminal separado e execute o cliente apontando para o nó desejado, troque a porta de acordo com o nó que deseja:


```bash
go run ./cmd/client/main.go localhost 8081
```

Você pode trocar o endereço e porta para interagir com qualquer nó do cluster.

No cliente, use os comandos:

- `put chave valor` — armazena uma chave/valor
- `get chave` — consulta uma chave
<!-- O comando delete ainda não está implementado -->
- `nodes` — lista os nós ativos
- `exit` — encerra o cliente


### 3. Usar os Comandos Interativos no Console

Após iniciar os nós, você pode interagir com o KV-Store usando os comandos set e get diretamente no console.

#### Comando put

No terminal do nó, insira uma chave e valor usando o comando put:

```bash
put chave valor
```

Esse comando armazena a chave *chave* com o valor *valor* no KV-Store. O valor será persistido no disco.

#### Comando get

Para consultar o valor associado a uma chave, use o comando get:
```bash
get chave
```

#### Comando exit

Para finalizar o nó, basta usar o comando:

```bash
exit
```

### 4. Testar a Persistência de Dados
Os dados são salvos automaticamente em arquivos JSON. Isso garante que as chaves e valores inseridos persistam mesmo após o fechamento do nó.

#### Passos para testar:
* Insira uma chave e valor usando o comando set.
* Feche a aplicação com o comando sair.
* Reinicie o nó usando go run main.go.
* Use o comando get para verificar se o valor da chave foi recuperado do disco.

### 5. Simular Conflitos e Resolução Automática
Se você rodar múltiplos nós e modificar os mesmos dados em diferentes nós, o KV-Store usará Vector Clocks para reconciliar os valores.

* Rode dois nós em terminais separados.
* Defina o mesmo valor em ambos os nós usando o comando set.
* Modifique o valor da chave em um nó.
* O sistema irá reconciliar automaticamente os valores entre os nós usando Vector Clocks.

### 6. Estrutura do Código
* **main.go**: Arquivo principal que inicia o servidor e integra os componentes.
* **cmd/server/main.go**: Inicialização dos nós e configuração do cluster.
* **cmd/client/main.go**: Cliente interativo para comandos PUT/GET/NODES.
* **internal/store**:
    * **kvstore.go**: KV-Store distribuído, persistência, reconciliação e propagação de dados.
    * **gossip.go**: Gossip Protocol, roteamento, health check e propagação entre nós.
    * **hashing.go**: Consistent Hashing para distribuição de chaves.
    * **vectorclock/**: Lógica de Vector Clocks para conciliação de versões.

### 7. Referências

- [Dynamo: Amazon's Highly Available Key-value Store](https://www.cs.cornell.edu/courses/cs5414/2017fa/papers/dynamo.pdf)
