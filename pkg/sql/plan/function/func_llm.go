package function

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"io"
	"net/http"
)

type OllamaSingleEmbeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type OllamaMultipleEmbeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type OllamaEmbeddingResponse struct {
	Model           string      `json:"model"`
	Embeddings      [][]float32 `json:"embeddings"`
	TotalDuration   int64       `json:"total_duration"`
	LoadDuration    int64       `json:"load_duration"`
	PromptEvalCount int         `json:"prompt_eval_count"`
}

// Prepare & send the HTTP request, read the response body, return embeddings
func callOllamaService(requestBody []byte, proxy string) ([][]float32, error) {
	// Prepare & send the HTTP request
	req, err := http.NewRequest("POST", proxy, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("received non-200 response: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var embeddingResponse OllamaEmbeddingResponse
	err = json.Unmarshal(body, &embeddingResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response body: %v", err)
	}

	return embeddingResponse.Embeddings, nil
}

// take single input, make a POST request to Ollama API and return embedding
func getOllamaSingleEmbedding(input string, model string, proxy string) ([]float32, error) {
	payload := OllamaSingleEmbeddingRequest{
		Model: model,
		Input: input,
	}

	// Marshal the payload to JSON
	requestBody, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	embeddings, err := callOllamaService(requestBody, proxy)

	return embeddings[0], nil
}

// Embedding function
func EmbeddingOp(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		inputBytes, nullInput := source.GetStrValue(i)
		if nullInput {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}

		model, err := proc.GetResolveVariableFunc()("llm_embedding_model", true, false)
		if err != nil {
			return err
		}
		modelStr, ok := model.(string)
		if !ok {
			return fmt.Errorf("unexpected type for llm_embedding_model: %T", model)
		}

		proxy, err1 := proc.GetResolveVariableFunc()("ollama_server_proxy", true, false)
		if err1 != nil {
			return err1
		}
		proxyStr, ok := proxy.(string)
		if !ok {
			return fmt.Errorf("unexpected type for llm_embedding_model: %T", proxy)
		}

		ollamaModel, err1 := proc.GetResolveVariableFunc()("ollama_model", true, false)
		if err1 != nil {
			return err1
		}
		ollamaModelStr, ok := ollamaModel.(string)
		if !ok {
			return fmt.Errorf("unexpected type for llm_embedding_model: %T", ollamaModel)
		}

		input := string(inputBytes)
		var embeddingBytes []byte
		switch modelStr {
		case "ollama":
			embedding, err := getOllamaSingleEmbedding(input, ollamaModelStr, proxyStr)
			if err != nil {
				return err
			}
			embeddingBytes = types.ArrayToBytes[float32](embedding)
		default:
			return fmt.Errorf("unsupported embedding model: %s", modelStr)
		}

		if err := rs.AppendBytes(embeddingBytes, false); err != nil {
			return err
		}
	}
	return nil

}

// Embedding function
func EmbeddingDatalinkOp(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	proxy, err1 := proc.GetResolveVariableFunc()("ollama_server_proxy", true, false)
	if err1 != nil {
		return err1
	}
	proxyStr, ok := proxy.(string)
	if !ok {
		return fmt.Errorf("unexpected type for llm_embedding_model: %T", proxy)
	}

	ollamaModel, err1 := proc.GetResolveVariableFunc()("ollama_model", true, false)
	if err1 != nil {
		return err1
	}
	ollamaModelStr, ok := ollamaModel.(string)
	if !ok {
		return fmt.Errorf("unexpected type for llm_embedding_model: %T", ollamaModel)
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		inputBytes, nullInput := source.GetStrValue(i)
		if nullInput {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}

		// read file for datalink type
		filePath := util.UnsafeBytesToString(inputBytes)
		fs := proc.GetFileService()
		moUrl, offsetSize, _, err := types.ParseDatalink(filePath)
		if err != nil {
			return err
		}

		r, err := ReadFromFileOffsetSize(moUrl, fs, int64(offsetSize[0]), int64(offsetSize[1]))
		if err != nil {
			return err
		}
		defer r.Close()

		fileBytes, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		if len(fileBytes) == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			return nil
		}

		model, err := proc.GetResolveVariableFunc()("llm_embedding_model", true, false)
		if err != nil {
			return err
		}
		modelStr, ok := model.(string)
		if !ok {
			return fmt.Errorf("unexpected type for llm_embedding_model: %T", model)
		}

		input := string(fileBytes)
		var embeddingBytes []byte
		switch modelStr {
		case "ollama":
			embedding, err := getOllamaSingleEmbedding(input, ollamaModelStr, proxyStr)
			if err != nil {
				return err
			}
			embeddingBytes = types.ArrayToBytes[float32](embedding)
		default:
			return fmt.Errorf("unsupported embedding model: %s", modelStr)
		}

		if err := rs.AppendBytes(embeddingBytes, false); err != nil {
			return err
		}
	}
	return nil

}
