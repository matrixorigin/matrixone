package function

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"io"
	"strconv"
	"strings"
)

// LLM_CHUNK
// Fixed width chunking
func fixedWidthChunk(text string, width int) [][]interface{} {
	var chunks [][]interface{}
	runes := []rune(text)
	for i := 0; i < len(runes); i += width {
		end := i + width
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, []interface{}{i, end - i, string(runes[i:end])})
	}
	return chunks
}

// Sentence-based chunking
func sentenceChunk(text string) [][]interface{} {
	var chunks [][]interface{}
	var start int
	for i, r := range text {
		if r == '.' || r == '!' || r == '?' {
			chunk := text[start : i+1]
			chunks = append(chunks, []interface{}{start, len(chunk), chunk})
			start = i + 1
		}
	}
	if start < len(text) {
		chunk := text[start:]
		chunks = append(chunks, []interface{}{start, len(chunk), chunk})
	}
	return chunks
}

// Paragraph-based chunking
func paragraphChunk(text string) [][]interface{} {
	var chunks [][]interface{}
	paragraphs := strings.Split(text, "\n")
	var start int
	for _, paragraph := range paragraphs {
		chunk := paragraph + "\n"
		chunks = append(chunks, []interface{}{start, len(chunk), chunk})
		start += len(chunk)
	}
	return chunks
}

func ChunkString(text, mode string) string {
	modeParts := strings.Split(mode, ";")
	for i := range modeParts {
		modeParts[i] = strings.TrimSpace(modeParts[i])
	}
	var chunks [][]interface{}
	if len(modeParts) == 2 && modeParts[0] == "fixed_width" {
		width, err := strconv.Atoi(modeParts[1])
		if err != nil {
			return ""
		}
		chunks = fixedWidthChunk(text, width)
	} else {
		switch modeParts[0] {
		case "sentence":
			chunks = sentenceChunk(text)
		case "paragraph":
			chunks = paragraphChunk(text)
		default:
			return ""
		}
	}

	var chunkStrings []string
	for _, chunk := range chunks {
		chunkStrings = append(chunkStrings, fmt.Sprintf("[%d, %d, %q]", chunk[0], chunk[1], chunk[2]))
	}

	return "[" + strings.Join(chunkStrings, ", ") + "]"
}

func Chunk(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	chunkTypeParam := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		inputBytes, nullInput := source.GetStrValue(i)
		chunkTypeBytes, nullChunkType := chunkTypeParam.GetStrValue(i)

		if nullInput || nullChunkType {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}

		inputPath := util.UnsafeBytesToString(inputBytes)
		moUrl, _, _, err := types.ParseDatalink(inputPath)
		if err != nil {
			return err
		}

		var input string

		fs := proc.GetFileService()
		r, err := ReadFromFile(moUrl, fs)
		if err != nil {
			return err
		}
		defer r.Close()
		ctx, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if len(ctx) > 65536 /*blob size*/ {
			return moerr.NewInternalError(proc.Ctx, "Data too long for blob")
		}
		if len(ctx) == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			return nil
		}

		input = string(ctx)

		chunkType := string(chunkTypeBytes)
		resultStr := ChunkString(input, chunkType)
		if err := rs.AppendMustBytesValue([]byte(resultStr)); err != nil {
			return err
		}
	}

	return nil
}
