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
	"github.com/matrixorigin/matrixone/pkg/defines"
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

	// ContentHash is non-empty when the datalink is pinned (carries ?contenthash=).
	// In that case MoPath addresses an immutable CAS object in the SHARED service.
	ContentHash string
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

	dl := Datalink{Url: u, Offset: offsetSize[0], Size: offsetSize[1], MoPath: moUrl}
	// Derive ContentHash from the parsed MoPath so it always addresses the same
	// CAS object that ParseDatalink resolved (no split-brain on duplicate or
	// mixed-case contenthash params).
	if hash, ok := casHashFromKey(moUrl); ok {
		dl.ContentHash = hash
	}
	return dl, nil
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
	// A pinned (?contenthash=) datalink addresses an immutable CAS object, and its
	// MoPath is the internal CAS key rather than the original external path. Writing
	// through it would route to that CAS key and break the pinned-read contract, so
	// reject writes outright: pinned content is immutable.
	if d.ContentHash != "" {
		return nil, moerr.NewInternalErrorf(proc.Ctx,
			"cannot write to a pinned datalink (contenthash=%s): pinned content is immutable", d.ContentHash)
	}
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

	// 1. collect query params. Values are lower-cased; a sha256 hex digest is
	// already lower-case so the contenthash value is unaffected.
	urlParams := make(map[string]string)
	for k, v := range u.Query() {
		urlParams[strings.ToLower(k)] = strings.ToLower(v[0])
	}

	// 2. get size and offset from the query (apply to both live and pinned values)
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

	// 3. a pinned datalink (?contenthash=) addresses an immutable CAS object and
	// never resolves to the live external path, so historical snapshot bytes stay
	// reproducible. A missing CAS object surfaces as a read error rather than a
	// silent fall back to the live (possibly overwritten) file.
	//
	// The CAS key is namespaced by the calling account, resolved from the trusted
	// execution context (never from the URL). This binds the read to the caller's
	// account: a contenthash cannot be used to read another account's pinned bytes.
	if hash, ok := urlParams[ContentHashKey]; ok {
		if err = ValidateContentHash(hash); err != nil {
			return "", nil, err
		}
		accountID, err := contentHashAccountID(proc)
		if err != nil {
			return "", nil, err
		}
		return CASKey(accountID, hash), offsetSize, nil
	}

	// 4. live reference: resolve to the external file's current location
	var moUrl string
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

	return moUrl, offsetSize, nil
}

// contentHashAccountID resolves the account that owns a pinned datalink's CAS
// namespace from the trusted execution context. A pinned read/write requires a
// session account; resolving it from the URL would re-introduce the bearer-token
// problem the per-account namespace is meant to prevent.
func contentHashAccountID(proc *process.Process) (uint32, error) {
	if proc == nil {
		return 0, moerr.NewInternalErrorNoCtx("pinned datalink requires an execution context to resolve the account")
	}
	return defines.GetAccountId(proc.Ctx)
}

func (d Datalink) NewReadCloser(proc *process.Process) (io.ReadCloser, error) {
	if d.ContentHash != "" {
		// pinned value: read the immutable CAS object directly from the SHARED
		// service. SHARED may be a plain FileService (e.g. LocalFS in standalone)
		// that does not implement ETLFileService, so we must not route through
		// GetForETL here. A missing object surfaces as a read error rather than a
		// silent fall back to the live (possibly overwritten) file.
		fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
		if err != nil {
			return nil, err
		}
		var r io.ReadCloser
		vec := fileservice.IOVector{
			FilePath: d.MoPath,
			Entries: []fileservice.IOEntry{
				0: {
					Offset:            d.Offset,
					Size:              d.Size,
					ReadCloserForRead: &r,
				},
			},
		}
		if err = fs.Read(proc.Ctx, &vec); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				// Key the error on the content hash, not the internal CAS storage
				// path: the path embeds the file-service layout and the account id
				// (non-deterministic), while the hash is what the user supplied.
				return nil, moerr.NewFileNotFound(proc.Ctx, d.ContentHash)
			}
			return nil, err
		}
		return r, nil
	}

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

// StatSize returns the byte size of the referenced object, transparently
// handling both live references and pinned (content-addressed) values.
func (d Datalink) StatSize(proc *process.Process) (int64, error) {
	if d.ContentHash != "" {
		fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
		if err != nil {
			return 0, err
		}
		entry, err := fs.StatFile(proc.Ctx, d.MoPath)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				return 0, moerr.NewFileNotFound(proc.Ctx, d.ContentHash)
			}
			return 0, err
		}
		return entry.Size, nil
	}

	etlFS, readPath, err := fileservice.GetForETL(proc.Ctx, proc.GetFileService(), d.MoPath)
	if err != nil {
		return 0, err
	}
	entry, err := etlFS.StatFile(proc.Ctx, readPath)
	if err != nil {
		return 0, err
	}
	return entry.Size, nil
}
