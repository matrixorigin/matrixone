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
	path   fileservice.Path
}

func NewDatalink(aurl string, proc *process.Process) (Datalink, error) {
	d, err := parseDatalink(aurl, proc)
	if err != nil {
		return Datalink{}, err
	}
	if err = process.AuthorizeSQLFileAccessPath(proc, d.path); err != nil {
		return Datalink{}, err
	}
	return d, nil
}

// NewAsyncDatalink admits only sources readable without a historical caller role.
// Parsing and stage resolution are shared with ordinary Datalinks; no I/O occurs.
func NewAsyncDatalink(aurl string, proc *process.Process) (Datalink, error) {
	d, err := parseDatalink(aurl, proc)
	if err != nil {
		return Datalink{}, err
	}
	if err = d.validateAsyncSource(); err != nil {
		return Datalink{}, err
	}
	return d, nil
}

func parseDatalink(aurl string, proc *process.Process) (Datalink, error) {

	u, err := url.Parse(aurl)
	if err != nil {
		return Datalink{}, err
	}

	moUrl, offset, size, err := parseDatalinkURL(u, proc)
	if err != nil {
		return Datalink{}, err
	}
	path, err := fileservice.ParsePath(moUrl)
	if err != nil {
		return Datalink{}, err
	}
	return Datalink{
		Url:    u,
		Offset: offset,
		Size:   size,
		MoPath: moUrl,
		path:   path,
	}, nil
}

// validateAsyncSource admits a source that a role-less CDC consumer can read.
// It classifies the resolved provider without constructing it or reading data.
// Call it on the same Datalink used for acquisition during an initial build.
func (d Datalink) validateAsyncSource() error {
	path, err := d.parsedPath()
	if err != nil {
		return err
	}
	return validateAsyncPath(path)
}

func validateAsyncPath(path fileservice.Path) error {
	local, err := fileservice.SQLFilePathRequiresAdmin(path)
	if err != nil {
		return err
	}
	if path.Service == "" || len(path.ServiceArguments) == 0 || local {
		return moerr.NewNotSupportedNoCtx("asynchronous indexes require remote Datalink sources; read local documents into a TEXT column with load_text before indexing")
	}
	return nil
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
	return fileservice.NewFileServiceWriterWithFileService(proc.GetFileService(), d.MoPath, process.SQLFileAccessContext(proc.Ctx, proc))
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
	moURL, offset, size, err := parseDatalinkURL(u, proc)
	if err != nil {
		return "", nil, err
	}
	return moURL, []int64{offset, size}, nil
}

// ValidateDatalink applies the legacy CAST checks without materializing output
// that the caller discards. It does not add file-path or access-policy checks.
func ValidateDatalink(aurl string, proc *process.Process) error {
	u, err := url.Parse(aurl)
	if err != nil {
		return err
	}
	var scratch [7]string
	if _, _, err = resolveDatalinkURL(u, proc, scratch[:0]); err != nil {
		return err
	}
	_, _, err = parseDatalinkRange(u)
	return err
}

// ValidateAsyncDatalinkSource checks a final stored value using the same
// resolver and policy as NewAsyncDatalink, without constructing an I/O object.
func ValidateAsyncDatalinkSource(aurl string, proc *process.Process) error {
	u, err := url.Parse(aurl)
	if err != nil {
		return err
	}
	var scratch [7]string
	fields, file, err := resolveDatalinkURL(u, proc, scratch[:0])
	if err != nil {
		return err
	}
	if _, _, err = parseDatalinkRange(u); err != nil {
		return err
	}
	path, err := fileservice.ParsePathFromServiceFields(fields, file)
	if err != nil {
		return err
	}
	return validateAsyncPath(path)
}

func parseDatalinkURL(u *url.URL, proc *process.Process) (string, int64, int64, error) {
	var scratch [7]string
	fields, file, err := resolveDatalinkURL(u, proc, scratch[:0])
	if err != nil {
		return "", 0, 0, err
	}
	moURL, err := fileservice.EncodeServicePath(fields, file)
	if err != nil {
		return "", 0, 0, err
	}
	offset, size, err := parseDatalinkRange(u)
	if err != nil {
		return "", 0, 0, err
	}
	return moURL, offset, size, nil
}

func resolveDatalinkURL(u *url.URL, proc *process.Process, fields []string) ([]string, string, error) {
	switch u.Scheme {
	case stage.FILE_PROTOCOL:
		return fields, strings.Join([]string{u.Host, u.Path}, ""), nil
	case stage.HDFS_PROTOCOL:
		return append(fields, "hdfs", "endpoint="+u.Host), u.Path, nil
	case stage.STAGE_PROTOCOL:
		source, err := stageutil.UrlToStageDefFromURL(u, proc)
		if err != nil {
			return nil, "", err
		}
		fields, file, _, err := source.AppendPathParts(fields)
		return fields, file, err
	default:
		return nil, "", moerr.NewNYINoCtxf("unsupported url scheme %s", u.Scheme)
	}
}

func parseDatalinkRange(u *url.URL) (int64, int64, error) {
	var err error
	// Keep the public range slice at the public boundary. Most row values have
	// no query and need neither a range allocation nor an empty query map.
	offset, size := int64(0), int64(-1)
	if u.RawQuery != "" {
		urlParams := make(map[string]string)
		for k, v := range u.Query() {
			urlParams[strings.ToLower(k)] = strings.ToLower(v[0])
		}
		if _, ok := urlParams["offset"]; ok {
			if offset, err = strconv.ParseInt(urlParams["offset"], 10, 64); err != nil {
				return 0, 0, err
			}
		}
		if _, ok := urlParams["size"]; ok {
			if size, err = strconv.ParseInt(urlParams["size"], 10, 64); err != nil {
				return 0, 0, err
			}
		}
	}
	if offset < 0 {
		return 0, 0, moerr.NewInternalErrorNoCtx("offset cannot be negative")
	}

	if size < -1 {
		return 0, 0, moerr.NewInternalErrorNoCtx("size cannot be less than -1")
	}

	return offset, size, nil
}

func (d Datalink) NewReadCloser(proc *process.Process) (io.ReadCloser, error) {
	path, err := d.parsedPath()
	if err != nil {
		return nil, err
	}
	fs := proc.GetFileService()
	fs, readPath, err := fileservice.GetForETLWithParsedPath(process.SQLFileAccessContext(proc.Ctx, proc), fs, d.MoPath, path)
	if fs == nil || err != nil {
		if fs != nil && d.ownsService(path) {
			fs.Close(proc.Ctx)
		}
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
		if d.ownsService(path) {
			fs.Close(proc.Ctx)
		}
		return nil, err
	}
	if d.ownsService(path) {
		return fileservice.NewReadCloserWithOwnedFileService(proc.Ctx, r, fs), nil
	}
	return r, nil
}

// StatFile authorizes and stats the resolved target while closing a temporary
// ETL service on every exit path. Configured named services are borrowed and
// remain owned by the process registry.
func (d Datalink) StatFile(proc *process.Process) (*fileservice.DirEntry, error) {
	path, err := d.parsedPath()
	if err != nil {
		return nil, err
	}
	fs, readPath, err := fileservice.GetForETLWithParsedPath(process.SQLFileAccessContext(proc.Ctx, proc), proc.GetFileService(), d.MoPath, path)
	if fs == nil || err != nil {
		if fs != nil && d.ownsService(path) {
			fs.Close(proc.Ctx)
		}
		return nil, err
	}
	if d.ownsService(path) {
		defer fs.Close(proc.Ctx)
	}
	return fs.StatFile(proc.Ctx, readPath)
}

func (d Datalink) ownsService(path fileservice.Path) bool {
	return path.Service == "" || len(path.ServiceArguments) > 0
}

func (d Datalink) parsedPath() (fileservice.Path, error) {
	if d.path.File != "" || d.path.Service != "" || len(d.path.ServiceArguments) != 0 {
		return d.path, nil
	}
	return fileservice.ParsePath(d.MoPath)
}
