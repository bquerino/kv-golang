package store

// Constantes de diretório de persistência
const pageDir = "pages"

// writeToPage removido: persistência agora é feita via log append-only em kvstore.go

// Função que retorna o caminho da página atual no diretório de persistência
// getCurrentPagePath removido: não utilizado com log append-only
