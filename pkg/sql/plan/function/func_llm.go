package function

import (
	"context"
	"github.com/ledongthuc/pdf"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

// LLMExtractText function
func LLMExtractText(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	input := vector.GenerateFunctionStrParameter(parameters[0])
	output := vector.GenerateFunctionStrParameter(parameters[1])
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

		inputPath := util.UnsafeBytesToString(inputBytes)
		outputPath := util.UnsafeBytesToString(outputBytes)

		moUrl, _, _, err := types.ParseDatalink(inputPath)
		if err != nil {
			return err
		}
		outputPathUrl, _, _, err := types.ParseDatalink(outputPath)
		if err != nil {
			return err
		}

		success := extractTextFromPdfAndWriteToFile(moUrl, outputPathUrl, proc)

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

func extractTextFromPdfAndWriteToFile(pdfPath string, txtPath string, proc *process.Process) bool {
	// read PDF to string
	content, err := readPdfToString(pdfPath)
	if err != nil {
		return false
	}

	// file service and write file
	ctx := context.TODO()
	fs, readPath, err := fileservice.GetForETL(ctx, proc.Base.FileService, txtPath)

	// delete the file if txt file exist because Write() only works when a file does not exist
	_, err = fs.StatFile(ctx, readPath)
	if err == nil {
		err1 := fs.Delete(ctx, readPath)
		if err1 != nil {
			return false
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
		return false
	}
	return true
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
