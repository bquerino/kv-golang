package store

import (
	"fmt"
	"os"
	"path/filepath"
)

const pageSize = 1024 // Definindo o tamanho máximo de cada página em bytes

// getCurrentPagePath retorna o caminho da página atual
func (kv *KVStore) getCurrentPagePath() string {
	return filepath.Join(pageDir, fmt.Sprintf("page_%d.txt", kv.pageIdx))
}

// initializePages inicializa a página atual para garantir que o armazenamento funcione
func (kv *KVStore) initializePages() {
	// Criar diretório de páginas se ainda não existir
	if err := os.MkdirAll(pageDir, os.ModePerm); err != nil {
		fmt.Println("Erro ao criar diretório de páginas:", err)
	}
}
