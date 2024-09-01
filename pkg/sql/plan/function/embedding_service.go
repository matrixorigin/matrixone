package function

type EmbeddingService interface {
	GetEmbedding(input string, model string, proxy string) ([]float32, error)
}

type OllamaEmbeddingService struct{}

func (o *OllamaEmbeddingService) GetEmbedding(input string, model string, proxy string) ([]float32, error) {
	return getOllamaSingleEmbedding(input, model, proxy)
}
