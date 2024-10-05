package store

import (
	"fmt"
	"os"
)

// Persistência de dados em disco
const pageDir = "pages"

// writeToPage escreve os dados na página atual. Se a página estiver cheia, cria uma nova página.
func (kv *KVStore) writeToPage(data string) {
	pagePath := kv.getCurrentPagePath()

	// Verifica o tamanho da página atual
	info, err := os.Stat(pagePath)
	if err == nil && info.Size() >= pageSize {
		kv.pageIdx++
		pagePath = kv.getCurrentPagePath()
	}

	// Abrir ou criar o arquivo da página
	file, err := os.OpenFile(pagePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Erro ao abrir página:", err)
		return
	}
	defer file.Close()

	// Escrever os dados no arquivo
	if _, err := file.WriteString(data); err != nil {
		fmt.Println("Erro ao escrever na página:", err)
	}
}
