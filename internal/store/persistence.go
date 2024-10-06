package store

import (
	"fmt"
	"log"
)

// Constantes de diretório de persistência
const pageDir = "pages"

// writeToPage escreve os dados na página atual usando o PageManager.
// Se a página atual estiver cheia, cria uma nova página.
func (kv *KeyValueStore) writeToPage(key, value string) {
	// Aloca uma nova página se necessário
	page := kv.PageManager.AllocatePage()

	// Constrói a string de dados para escrita (formato simples: "chave:valor")
	data := fmt.Sprintf("%s:%s\n", key, value)

	// Converte o dado para bytes
	binaryData := []byte(data)

	// Verifica se há espaço suficiente na página para escrever o dado
	if page.Used+len(binaryData) > PageSize {
		// Se a página estiver cheia, persiste a página e aloca uma nova
		err := kv.PageManager.WritePage(page)
		if err != nil {
			log.Printf("Error writing page: %v", err)
			return
		}

		// Aloca uma nova página
		page = kv.PageManager.AllocatePage()
	}

	// Escreve os dados na página
	copy(page.Buffer[page.Used:], binaryData)
	page.Used += len(binaryData)

	// Persiste a página atualizada
	err := kv.PageManager.WritePage(page)
	if err != nil {
		log.Printf("Error writing page: %v", err)
	}
}

// Função que retorna o caminho da página atual no diretório de persistência
func (kv *KeyValueStore) getCurrentPagePath() string {
	// O arquivo de página atual pode ser identificado pelo índice da página
	pageFile := fmt.Sprintf("%s/page_%d.dat", pageDir, kv.PageManager.NextPageID)
	return pageFile
}
