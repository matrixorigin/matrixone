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
	"encoding/csv"
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

	// When the datalink carries an explicit size (e.g. file://...?size=N, which the
	// index stores emit for every load_file chunk), read exactly that into a
	// pre-sized buffer instead of io.ReadAll. io.ReadAll grows-and-copies (several
	// intermediate allocations + up to 2x over-allocation, all short-lived heap
	// garbage) — costly when load_file is evaluated per row in a batched INSERT.
	if d.Size > 0 {
		buf := make([]byte, d.Size)
		n, rerr := io.ReadFull(r, buf)
		if rerr == io.ErrUnexpectedEOF || rerr == io.EOF {
			return buf[:n], nil // size ran past EOF: return what was read (io.ReadAll semantics)
		}
		if rerr != nil {
			return nil, rerr
		}
		return buf, nil
	}

	fileBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return fileBytes, nil
}

// ReadInto reads the datalink's `size` bytes into buf, growing buf only when it is
// too small, and returns the filled prefix (aliasing buf). Unlike GetBytes it does
// NOT allocate a fresh slice per call: a caller that copies the result out before
// the next call — e.g. load_file, whose AppendBytes copies into the result vector —
// can pass the same buf back every row. A large batched INSERT then holds ONE
// reusable buffer instead of piling up one ~chunk-sized heap allocation per row for
// the GC to chase (which is what forced the per-INSERT value cap).
func (d Datalink) ReadInto(proc *process.Process, buf []byte, size int64) ([]byte, error) {
	r, err := d.NewReadCloser(proc)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if size <= 0 {
		return buf[:0], nil
	}
	if int64(cap(buf)) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	// io.ReadFull overwrites buf[0:size] exactly, so a reslice from a larger prior
	// read leaves no stale tail.
	n, rerr := io.ReadFull(r, buf)
	if rerr == io.ErrUnexpectedEOF || rerr == io.EOF {
		return buf[:n], nil // size ran past EOF: return what was read (io.ReadAll semantics)
	}
	if rerr != nil {
		return nil, rerr
	}
	return buf, nil
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
	case stage.HDFS_PROTOCOL:
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		opts := []string{"hdfs", "endpoint=" + u.Host}

		if err = w.Write(opts); err != nil {
			return "", nil, err
		}
		w.Flush()
		moUrl = fileservice.JoinPath(buf.String(), u.Path)
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
