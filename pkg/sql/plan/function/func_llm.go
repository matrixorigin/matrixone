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
type Chunk struct {
	Start  int
	Length int
	Text   string
}

func fixedWidthChunk(text string, width int) []Chunk {
	var chunks []Chunk
	runes := []rune(text)
	for i := 0; i < len(runes); i += width {
		end := i + width
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, Chunk{Start: i, Length: end - i, Text: string(runes[i:end])})
	}
	return chunks
}

// Sentence-based chunking
func sentenceChunk(text string) []Chunk {
	var chunks []Chunk
	var start int
	for i, r := range text {
		if r == '.' || r == '!' || r == '?' {
			chunk := text[start : i+1]
			chunks = append(chunks, Chunk{Start: start, Length: len(chunk), Text: chunk})
			start = i + 1
		}
	}
	if start < len(text) {
		chunk := text[start:]
		chunks = append(chunks, Chunk{Start: start, Length: len(chunk), Text: chunk})
	}
	return chunks
}

// document-based chunking
func documentChunk(text string) []Chunk {
	var chunks []Chunk
	var start = 0
	chunks = append(chunks, Chunk{Start: start, Length: len(text), Text: text})
	return chunks
}

// Paragraph-based chunking
func paragraphChunk(text string) []Chunk {
	var chunks []Chunk
	paragraphs := strings.Split(text, "\n")
	var start int
	for _, paragraph := range paragraphs {
		chunk := paragraph + "\n"
		chunks = append(chunks, Chunk{Start: start, Length: len(chunk), Text: chunk})
		start += len(chunk)
	}
	return chunks
}

func ChunkString(text, mode string) (string, error) {
	modeParts := strings.Split(mode, ";")
	for i := range modeParts {
		modeParts[i] = strings.TrimSpace(modeParts[i])
	}
	var chunks []Chunk
	if len(modeParts) == 2 && modeParts[0] == "fixed_width" {
		width, err := strconv.Atoi(modeParts[1])
		if width < 0 || err != nil {
			return "", moerr.NewInvalidInputNoCtxf("'%s' is not a valid chunk strategy", mode)
		}
		chunks = fixedWidthChunk(text, width)
	} else {
		switch modeParts[0] {
		case "sentence":
			chunks = sentenceChunk(text)
		case "paragraph":
			chunks = paragraphChunk(text)
		case "document":
			chunks = documentChunk(text)
		default:
			return "", moerr.NewInvalidInputNoCtxf("'%s' is not a valid chunk strategy", mode)
		}
	}

	var chunkStrings []string
	for _, chunk := range chunks {
		chunkStrings = append(chunkStrings, fmt.Sprintf("[%d, %d, %q]", chunk.Start, chunk.Length, chunk.Text))
	}

	return "[" + strings.Join(chunkStrings, ", ") + "]", nil
}

func LLMChunk(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
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

		input = string(ctx)
		if len(input) == 0 {
			return moerr.NewInvalidInputNoCtxf("Empty file is not valid")
		}

		chunkType := string(chunkTypeBytes)
		resultStr, err := ChunkString(input, chunkType)
		if err != nil {
			return err
		}
		if err := rs.AppendMustBytesValue([]byte(resultStr)); err != nil {
			return err
		}
	}

	return nil
}
