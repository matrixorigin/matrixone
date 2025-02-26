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

package datalink

import (
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/datalink/docx"
	"github.com/matrixorigin/matrixone/pkg/datalink/pdf"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/stage/stageutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Datalink struct {
	Url    *url.URL
	Offset int64
	Size   int64
	MoPath string
}

func NewDatalink(aurl string, proc *process.Process) (Datalink, error) {

	u, err := url.Parse(aurl)
	if err != nil {
		return Datalink{}, err
	}

	moUrl, offsetSize, err := ParseDatalink(aurl, proc)
	if err != nil {
		return Datalink{}, err
	}
	return Datalink{Url: u, Offset: int64(offsetSize[0]), Size: int64(offsetSize[1]), MoPath: moUrl}, nil
}

func (d Datalink) GetBytes(proc *process.Process) ([]byte, error) {
	r, err := d.NewReadCloser(proc)
	if err != nil {
		return nil, err
	}

	defer r.Close()

	fileBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return fileBytes, nil
}

func (d Datalink) GetPlainText(proc *process.Process) ([]byte, error) {

	fileBytes, err := d.GetBytes(proc)
	if err != nil {
		return nil, err
	}

	ext := strings.ToLower(filepath.Ext(d.Url.Path))
	switch ext {
	case ".pdf":
		return pdf.GetPlainText(fileBytes)
	case ".docx":
		return docx.GetPlainText(fileBytes)
	default:
		return fileBytes, nil
	}
}

func (d Datalink) NewWriter(proc *process.Process) (*fileservice.FileServiceWriter, error) {

	return fileservice.NewFileServiceWriter(d.MoPath, proc.Ctx)
}

// ParseDatalink extracts data from a Datalink string
// and returns the Mo FS url, []int{offset,size}, fileType and error
// Mo FS url: The URL that is used by MO FS to access the file
// offsetSize: The offset and size of the file to be read
func ParseDatalink(fsPath string, proc *process.Process) (string, []int64, error) {
	u, err := url.Parse(fsPath)
	if err != nil {
		return "", nil, err
	}

	var moUrl string
	// 1. get moUrl from the path
	switch u.Scheme {
	case stage.FILE_PROTOCOL:
		moUrl = strings.Join([]string{u.Host, u.Path}, "")
	case stage.STAGE_PROTOCOL:
		moUrl, _, err = stageutil.UrlToPath(fsPath, proc)
		if err != nil {
			return "", nil, err
		}
	default:
		return "", nil, moerr.NewNYINoCtxf("unsupported url scheme %s", u.Scheme)
	}

	// 2. get size and offset from the query
	urlParams := make(map[string]string)
	for k, v := range u.Query() {
		urlParams[strings.ToLower(k)] = strings.ToLower(v[0])
	}
	offsetSize := []int64{0, -1}
	if _, ok := urlParams["offset"]; ok {
		if offsetSize[0], err = strconv.ParseInt(urlParams["offset"], 10, 64); err != nil {
			return "", nil, err
		}
	}
	if _, ok := urlParams["size"]; ok {
		if offsetSize[1], err = strconv.ParseInt(urlParams["size"], 10, 64); err != nil {
			return "", nil, err
		}
	}

	if offsetSize[0] < 0 {
		return "", nil, moerr.NewInternalErrorNoCtx("offset cannot be negative")
	}

	if offsetSize[1] < -1 {
		return "", nil, moerr.NewInternalErrorNoCtx("size cannot be less than -1")
	}

	return moUrl, offsetSize, nil
}

func (d Datalink) NewReadCloser(proc *process.Process) (io.ReadCloser, error) {
	fs := proc.GetFileService()
	fs, readPath, err := fileservice.GetForETL(proc.Ctx, fs, d.MoPath)
	if fs == nil || err != nil {
		return nil, err
	}
	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            d.Offset, //0 - default
				Size:              d.Size,   //-1 - default
				ReadCloserForRead: &r,
			},
		},
	}
	err = fs.Read(proc.Ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}
