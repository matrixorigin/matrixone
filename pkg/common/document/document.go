package document

import (
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/document/docx"
	"github.com/matrixorigin/matrixone/pkg/common/document/pdf"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func GetContentFromURL(aurl string, proc *process.Process) ([]byte, error) {

	fs := proc.GetFileService()
	moUrl, offsetSize, err := function.ParseDatalink(aurl, proc)
	if err != nil {
		return nil, err
	}

	r, err := function.ReadFromFileOffsetSize(moUrl, fs, int64(offsetSize[0]), int64(offsetSize[1]))
	if err != nil {
		return nil, err
	}

	defer r.Close()

	fileBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(aurl)
	if err != nil {
		return nil, err
	}

	ext := strings.ToLower(filepath.Ext(u.Path))
	switch ext {
	case ".pdf":
		return pdf.GetContent(fileBytes)
	case ".docx":
		return docx.GetContent(fileBytes)
	default:
		return fileBytes, nil
	}
}
