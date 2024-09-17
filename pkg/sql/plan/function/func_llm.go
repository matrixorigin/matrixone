package function

import (
	"context"
	"fmt"
	"github.com/ledongthuc/pdf"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
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

// LLMExtractText function
func LLMExtractText(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	input := vector.GenerateFunctionStrParameter(parameters[0])
	output := vector.GenerateFunctionStrParameter(parameters[1])
	extractorType := vector.GenerateFunctionStrParameter(parameters[2])
	rs := vector.MustFunctionResult[bool](result)

	rowCount := uint64(length)

	for i := uint64(0); i < rowCount; i++ {
		inputBytes, nullInput := input.GetStrValue(i)
		if nullInput {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}

		outputBytes, nullInput2 := output.GetStrValue(i)
		if nullInput2 {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}

		extractorTypeBytes, nullInput3 := extractorType.GetStrValue(i)
		if nullInput3 {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}

		inputPath := util.UnsafeBytesToString(inputBytes)
		outputPath := util.UnsafeBytesToString(outputBytes)
		extractorTypeString := util.UnsafeBytesToString(extractorTypeBytes)

		moUrl, _, ext, err := types.ParseDatalink(inputPath)
		if err != nil {
			return err
		}
		if "."+extractorTypeString != ext {
			return moerr.NewInvalidInputNoCtxf("File type and extractor type are not equal.")
		}
		if ext != ".pdf" {
			return moerr.NewInvalidInputNoCtxf("Only pdf file supported.")
		}

		outputPathUrl, _, _, err := types.ParseDatalink(outputPath)
		if err != nil {
			return err
		}

		success, err := extractTextFromPdfAndWriteToFile(moUrl, outputPathUrl, proc)
		if err != nil {
			return err
		}

		// return whether the process completes successfully
		if success {
			if err := rs.Append(true, false); err != nil {
				return err
			}
		} else {
			if err := rs.Append(false, false); err != nil {
				return err
			}
		}

	}

	return nil
}

func extractTextFromPdfAndWriteToFile(pdfPath string, txtPath string, proc *process.Process) (bool, error) {
	// read PDF to string
	content, err := readPdfToString(pdfPath)
	if err != nil {
		return false, moerr.NewInvalidInputNoCtxf("Invalid PDF input.")
	}

	// file service and write file
	ctx := context.TODO()
	fs, readPath, err := fileservice.GetForETL(ctx, proc.Base.FileService, txtPath)

	// delete the file if txt file exist because Write() only works when a file does not exist
	_, err = fs.StatFile(ctx, readPath)
	if err == nil {
		err1 := fs.Delete(ctx, readPath)
		if err1 != nil {
			return false, moerr.NewInvalidInputNoCtxf("Cannot remove file %s", readPath)
		}
	}

	_, err = fileservice.DoWithRetry(
		"BackupWrite",
		func() (int, error) {
			return 0, fs.Write(ctx, fileservice.IOVector{
				FilePath: readPath,
				Entries: []fileservice.IOEntry{
					{
						Offset: 0,
						Size:   int64(len(content)),
						Data:   []byte(content),
					},
				},
			})
		},
		64,
		fileservice.IsRetryableError,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}

func isSameSentence(current, last pdf.Text) bool {
	return strings.TrimSpace(current.S) != "" &&
		last.Font == current.Font &&
		last.FontSize == current.FontSize &&
		last.X == current.X &&
		last.Y == current.Y
}

func readPdfToString(path string) (string, error) {
	f, r, err := pdf.Open(path)
	if err != nil {
		return "", err
	}
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	var textBuilder strings.Builder
	totalPage := r.NumPage()

	for pageIndex := 1; pageIndex <= totalPage; pageIndex++ {
		p := r.Page(pageIndex)
		if p.V.IsNull() {
			continue
		}
		var lastTextStyle pdf.Text
		texts := p.Content().Text
		for _, text := range texts {
			if isSameSentence(text, lastTextStyle) {
				lastTextStyle.S += text.S
			} else {
				if lastTextStyle.S != "" {
					textBuilder.WriteString(lastTextStyle.S)
				}
				lastTextStyle = text
			}
		}
		if lastTextStyle.S != "" {
			textBuilder.WriteString(lastTextStyle.S + " ")
		}
	}

	return textBuilder.String(), nil
}

//func LLMTest(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
//	rs := vector.MustFunctionResult[types.Varlena](result)
//
//	v, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.InternalSQLExecutor)
//	if !ok {
//		return moerr.NewNotSupported(proc.Ctx, "no implement sqlExecutor")
//	}
//	exec := v.(executor.SQLExecutor)
//
//	selectObj := func(tbl *table.Table) (string, error) {
//		var result string
//		selectSQL := fmt.Sprintf("SELECT " +
//			"index_table_name " +
//			"FROM mo_catalog.mo_indexes " +
//			"WHERE algo_table_type = 'llmtable' " +
//			"AND name = 'idx1';")
//		opts := executor.Options{}.WithDatabase(tbl.Database).WithTimeZone(proc.GetSessionInfo().TimeZone)
//		res, err := exec.Exec(proc.Ctx, selectSQL, opts)
//		if err != nil {
//			return "", err
//		}
//		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
//			for i := 0; i < rows; i++ {
//				result += executor.GetStringRows(cols[0])[i]
//			}
//			return true
//		})
//		res.Close()
//		return result, nil
//	}
//
//	extractObj := func(tbl *table.Table, llmTable string) (string, error) {
//		var result string
//		extractSQL := fmt.Sprintf("SELECT chunk "+
//			"FROM `%s` "+
//			"ORDER BY l2_distance(embedding, llm_embedding(\"推荐阅读\n从下面的文章中选择一篇，开始您的 MatrixOne 之旅。如果您：\")) "+
//			"ASC LIMIT 1;",
//			llmTable)
//		opts := executor.Options{}.WithDatabase(tbl.Database)
//		res, err := exec.Exec(proc.Ctx, extractSQL, opts)
//		if err != nil {
//			return "", err
//		}
//		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
//			for i := 0; i < rows; i++ {
//				result += executor.GetStringRows(cols[0])[i]
//			}
//			return true
//		})
//		res.Close()
//		return result, nil
//	}
//
//	for i := uint64(0); i < uint64(length); i++ {
//		tables := table.GetAllTables()
//		llmTableNames := make(map[int]string)
//		for i, tbl := range tables {
//			llmTableName, err := selectObj(tbl)
//
//			if err != nil {
//				return err
//			}
//
//			llmTableNames[i] = llmTableName
//
//		}
//
//		for i, tbl := range tables {
//			llmTableName := llmTableNames[i]
//			result, err := extractObj(tbl, llmTableName)
//
//			if err != nil {
//				return err
//			}
//			rs.AppendMustBytesValue(util.UnsafeStringToBytes(result))
//		}
//
//	}
//
//	return nil
//}

func LLMAsk(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	v, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(proc.Ctx, "no implement sqlExecutor")
	}
	exec := v.(executor.SQLExecutor)

	extractObj := func(tbl *table.Table) (string, error) {
		var result string
		//extractSQL := fmt.Sprintf("SELECT chunk "+
		//	"FROM `%s` "+
		//	"ORDER BY l2_distance(embedding, llm_embedding(\"推荐阅读\n从下面的文章中选择一篇，开始您的 MatrixOne 之旅。如果您：\")) "+
		//	"ASC LIMIT 1;",
		//	llmTable)
		//extractSQL := fmt.Sprintf("select b from indextest3.t6;")
		extractSQL := fmt.Sprintf("SELECT chunk FROM `indextest3`.`__mo_index_secondary_0191feff-79bf-7799-a07c-6da193ad097a` ORDER BY l2_distance(embedding, llm_embedding(\"推荐阅读\n从下面的文章中选择一篇，开始您的 MatrixOne 之旅。如果您：\")) ASC LIMIT 1;")
		opts := executor.Options{}.WithDatabase(tbl.Database)
		res, err := exec.Exec(proc.Ctx, extractSQL, opts)
		if err != nil {
			return "", err
		}
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				result += executor.GetStringRows(cols[0])[i]
			}
			return true
		})
		res.Close()
		return result, nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		tables := table.GetAllTables()

		for _, tbl := range tables {
			//llmTableName := llmTableNames[i]
			result, err := extractObj(tbl)

			if err != nil {
				return err
			}
			rs.AppendMustBytesValue(util.UnsafeStringToBytes(result))
		}

	}

	return nil
}

func LLMAsk3Arguments(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	databaseInput := vector.GenerateFunctionStrParameter(parameters[0])
	indexInput := vector.GenerateFunctionStrParameter(parameters[1])
	questionInput := vector.GenerateFunctionStrParameter(parameters[2])

	v, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(proc.Ctx, "no implement sqlExecutor")
	}
	exec := v.(executor.SQLExecutor)

	selectObj := func(tbl *table.Table, llmIndexName string) (string, error) {
		var result string
		selectSQL := fmt.Sprintf("SELECT "+
			"index_table_name "+
			"FROM mo_catalog.mo_indexes "+
			"WHERE algo_table_type = 'llmtable' "+
			"AND name = '%s';",
			llmIndexName)
		opts := executor.Options{}.WithDatabase(tbl.Database).WithTimeZone(proc.GetSessionInfo().TimeZone)
		res, err := exec.Exec(proc.Ctx, selectSQL, opts)
		if err != nil {
			return "", err
		}
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				result += executor.GetStringRows(cols[0])[i]
			}
			return true
		})
		res.Close()
		return result, nil
	}

	extractObj := func(tbl *table.Table, llmDatabaseName string, llmTableName string, llmQuestion string) (string, error) {
		var result string
		//extractSQL := fmt.Sprintf("SELECT chunk "+
		//	"FROM `%s` "+
		//	"ORDER BY l2_distance(embedding, llm_embedding(\"推荐阅读\n从下面的文章中选择一篇，开始您的 MatrixOne 之旅。如果您：\")) "+
		//	"ASC LIMIT 1;",
		//	llmTable)
		//extractSQL := fmt.Sprintf("select b from indextest3.t6;")
		extractSQL := fmt.Sprintf("SELECT chunk "+
			"FROM `%s`.`%s` "+
			"ORDER BY l2_distance(embedding, llm_embedding(\"%s\")) "+
			"ASC LIMIT 1;",
			llmDatabaseName,
			llmTableName,
			llmQuestion)
		opts := executor.Options{}.WithDatabase(tbl.Database)
		res, err := exec.Exec(proc.Ctx, extractSQL, opts)
		if err != nil {
			return "", err
		}
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				result += executor.GetStringRows(cols[0])[i]
			}
			return true
		})
		res.Close()
		return result, nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		tables := table.GetAllTables()
		databaseInputBytes, null1 := databaseInput.GetStrValue(i)
		indexInputBytes, null2 := indexInput.GetStrValue(i)
		questionInputBytes, null3 := questionInput.GetStrValue(i)

		if null1 || null2 || null3 {
			return moerr.NewInvalidInputNoCtxf("Wrong Input Bytes.")
		}

		database := string(databaseInputBytes)
		index := string(indexInputBytes)
		question := string(questionInputBytes)

		llmTableNames := make(map[int]string)
		for i, tbl := range tables {
			llmTableName, err := selectObj(tbl, index)

			if err != nil {
				return err
			}

			llmTableNames[i] = llmTableName

		}

		for i, tbl := range tables {
			llmTableName := llmTableNames[i]
			result, err := extractObj(tbl, database, llmTableName, question)

			if err != nil {
				return err
			}
			rs.AppendMustBytesValue(util.UnsafeStringToBytes(result))
		}

	}

	return nil
}
