package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type EmbeddingService interface {
	GetEmbedding(input string, model string, proxy string) ([]float32, error)
}

type OllamaEmbeddingService struct{}

func (o *OllamaEmbeddingService) GetEmbedding(input string, model string, proxy string) ([]float32, error) {
	return getOllamaSingleEmbedding(input, model, proxy)
}

func NewEmbeddingService(modelStr string) (EmbeddingService, error) {
	switch modelStr {
	case "ollama":
		return &OllamaEmbeddingService{}, nil
	default:
		return nil, moerr.NewInvalidInputNoCtxf("'%s' is not a valid chunk strategy", modelStr)
	}
}

func getLLMGlobalVariable(proc *process.Process) (string, string, string, error) {
	platform, err := proc.GetResolveVariableFunc()("llm_embedding_platform", true, false)
	if err != nil {
		return "", "", "", err
	}
	platformStr, ok := platform.(string)
	if !ok {
		return "", "", "", moerr.NewInvalidInputf(proc.Ctx, "unexpected type for llm_embedding_platform: %T", platform)
	}

	proxy, err1 := proc.GetResolveVariableFunc()("llm_server_proxy", true, false)
	if err1 != nil {
		return "", "", "", err1
	}
	proxyStr, ok := proxy.(string)
	if !ok {
		return "", "", "", moerr.NewInvalidInputf(proc.Ctx, "unexpected type for llm_server_proxy: %T", proxy)
	}

	llmModel, err1 := proc.GetResolveVariableFunc()("llm_model", true, false)
	if err1 != nil {
		return "", "", "", err1
	}
	llmModelStr, ok := llmModel.(string)
	if !ok {
		return "", "", "", moerr.NewInvalidInputf(proc.Ctx, "unexpected type for llm_model: %T", llmModel)
	}

	return platformStr, proxyStr, llmModelStr, nil
}
