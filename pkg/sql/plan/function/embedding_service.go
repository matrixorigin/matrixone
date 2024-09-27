// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
