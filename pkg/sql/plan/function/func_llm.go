package function

import (
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

	platformStr, proxyStr, llmModelStr, err := getLLMGlobalVariable(proc)
	if err != nil {
		return err
	}

	embeddingService, err := NewEmbeddingService(platformStr)
	if err != nil {
		return err
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

		input := string(inputBytes)
		var embeddingBytes []byte

		embedding, err := embeddingService.GetEmbedding(input, llmModelStr, proxyStr)
		if err != nil {
			return err
		}
		embeddingBytes = types.ArrayToBytes[float32](embedding)

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

	platformStr, proxyStr, llmModelStr, err := getLLMGlobalVariable(proc)
	if err != nil {
		return err
	}

	embeddingService, err := NewEmbeddingService(platformStr)
	if err != nil {
		return err
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

		input := string(fileBytes)
		var embeddingBytes []byte

		embedding, err := embeddingService.GetEmbedding(input, llmModelStr, proxyStr)
		if err != nil {
			return err
		}
		embeddingBytes = types.ArrayToBytes[float32](embedding)

		if err := rs.AppendBytes(embeddingBytes, false); err != nil {
			return err
		}
	}
	return nil

}
