# KV-Store Distribuído com Gossip Protocol

Este projeto implementa um KV-Store (Key-Value Store) distribuído usando Go, com suporte para **Gossip Protocol** e **persistência em disco**. Ele foi projetado para funcionar em um ambiente distribuído com múltiplos nós que se comunicam entre si.

## Funcionalidades

- **Gossip Protocol**: Comunicação entre nós distribuídos para propagação de chaves e valores.
- **Persistência em disco**: Chaves e valores são salvos em arquivos locais, garantindo que os dados sejam recuperados após reiniciar o sistema.
- **Vector Clocks**: Controle de versões para garantir a consistência dos dados em ambientes distribuídos.
- **Resolução de Conflitos**: Quando há conflitos entre versões de dados, os valores são mesclados.

## Requisitos

- [Go](https://golang.org/dl/) (v1.16 ou superior)
- Um ambiente que permita múltiplas instâncias rodando (múltiplos terminais ou servidores).

## Como Testar o Projeto

### 1. Clonar o Repositório

Clone o repositório para a sua máquina:

```bash
git clone https://github.com/bquerino/kv-golang.git
cd kvstore-golang
```

### 2. Executar Múltiplos Nós

Para simular um ambiente distribuído com múltiplos nós, você precisará abrir diferentes terminais para rodar as instâncias.

**Terminal 1: Rodar o Nó 1**

Abra o primeiro terminal e execute o nó 1:

```bash
go run main.go
```

**Terminal 2: Rodar o Nó 2**

No segundo terminal, você pode rodar o nó 2:

```bash
go run main.go
```

### 3. Usar os Comandos Interativos no Console

Após iniciar os nós, você pode interagir com o KV-Store usando os comandos set e get diretamente no console.

#### Comando set

No terminal do nó, insira uma chave e valor usando o comando set:

```bash
set chave valor
```

Esse comando armazena a chave *chave* com o valor *valor* no KV-Store. O valor será persistido no disco.

#### Comando get

Para consultar o valor associado a uma chave, use o comando get:
```bash
get chave
```

#### Comando sair

Para finalizar o nó, basta usar o comando:

```bash
sair
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
* **main.go**: Arquivo principal que inicia os nós e permite a interação via console.
* **internal/store**:
    * **kvstore.go**: Implementação principal do KV-Store, incluindo persistência e lógica de reconciliação de dados.
    * **gossip.go**: Implementação do Gossip Protocol para comunicação entre os nós.
    * **persistence.go**: Funções auxiliares para salvar e carregar dados do disco.