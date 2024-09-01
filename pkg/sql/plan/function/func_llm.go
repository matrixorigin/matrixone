package function

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"io"
)

// Embedding function
func EmbeddingOp(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	// Initialize the Ollama embedding service
	embeddingService := &OllamaEmbeddingService{}

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
			embedding, err := embeddingService.GetEmbedding(input, ollamaModelStr, proxyStr)
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

	// Initialize the Ollama embedding service
	embeddingService := &OllamaEmbeddingService{}

	proxy, err := proc.GetResolveVariableFunc()("ollama_server_proxy", true, false)
	if err != nil {
		return err
	}
	proxyStr, ok := proxy.(string)
	if !ok {
		return moerr.NewInvalidInputf(proc.Ctx, "unexpected type for llm_embedding_model: %T", proxy)
	}

	ollamaModel, err1 := proc.GetResolveVariableFunc()("ollama_model", true, false)
	if err1 != nil {
		return err1
	}
	ollamaModelStr, ok := ollamaModel.(string)
	if !ok {
		return moerr.NewInvalidInputf(proc.Ctx, "unexpected type for llm_embedding_model: %T", ollamaModel)
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
		moUrl, _, _, err := types.ParseDatalink(filePath)
		if err != nil {
			return err
		}

		r, err := ReadFromFileOffsetSize(moUrl, fs, 0, -1)
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
			return moerr.NewInvalidInputf(proc.Ctx, "unexpected type for llm_embedding_model: %T", model)
		}

		input := string(fileBytes)
		var embeddingBytes []byte
		switch modelStr {
		case "ollama":
			embedding, err := embeddingService.GetEmbedding(input, ollamaModelStr, proxyStr)
			if err != nil {
				return err
			}
			embeddingBytes = types.ArrayToBytes[float32](embedding)
		default:
			return moerr.NewInvalidInputf(proc.Ctx, "unsupported embedding model: %s", modelStr)
		}

		if err := rs.AppendBytes(embeddingBytes, false); err != nil {
			return err
		}
	}
	return nil

}
