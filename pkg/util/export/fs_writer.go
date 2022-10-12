// Copyright 2022 Matrix Origin
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

package export

import (
	"context"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

const etlFileServiceName = "ETL"

var _ stringWriter = (*FSWriter)(nil)

type FSWriter struct {
	ctx context.Context         // New args
	fs  fileservice.FileService // New args

	mux      sync.Mutex
	prefix   string // see WithPrefix, as filename prefix, see GetNewFileName
	dir      string // see WithDir, default: ""
	nodeUUID string // see WithNode
	nodeType string // see WithNode
	filename string // see GetNewFileName, auto generated

	fileServiceName string // see WithFileServiceName

	offset int // see Write, should not have size bigger than 2GB
}

type FSWriterOption interface {
	Apply(*FSWriter)
}

func NewFSWriter(ctx context.Context, fs fileservice.FileService, opts ...FSWriterOption) *FSWriter {
	w := &FSWriter{
		ctx:      ctx,
		fs:       fs,
		prefix:   "info",
		dir:      "",
		nodeUUID: "0",
		nodeType: "standalone",

		fileServiceName: etlFileServiceName,
	}
	for _, o := range opts {
		o.Apply(w)
	}
	w.filename = w.GetNewFileName(util.Now())
	return w
}

type fsWriterOptionFunc func(*FSWriter)

func (f fsWriterOptionFunc) Apply(w *FSWriter) {
	f(w)
}

func WithPrefix(item batchpipe.HasName) fsWriterOptionFunc {
	return fsWriterOptionFunc(func(w *FSWriter) {
		w.prefix = item.GetName()
	})
}

func WithDir(dir string) fsWriterOptionFunc {
	return fsWriterOptionFunc(func(w *FSWriter) {
		w.dir = dir
	})
}
func WithNode(uuid, nodeType string) fsWriterOptionFunc {
	return fsWriterOptionFunc(func(w *FSWriter) {
		w.nodeUUID, w.nodeType = uuid, nodeType
	})
}

func WithFileServiceName(serviceName string) fsWriterOptionFunc {
	return fsWriterOptionFunc(func(w *FSWriter) {
		w.fileServiceName = serviceName
	})
}

func (w *FSWriter) GetNewFileName(ts time.Time) string {
	return fmt.Sprintf(`%s_%s_%s_%s`, w.prefix, w.nodeUUID, w.nodeType, ts.Format("20060102.150405.000000"))
}

// Write implement io.Writer, Please execute in series
func (w *FSWriter) Write(p []byte) (n int, err error) {
	w.mux.Lock()
	defer w.mux.Unlock()
	n = len(p)
	mkdirTried := false
mkdirRetry:
	if err = w.fs.Write(w.ctx, fileservice.IOVector{
		// like: etl:store/system/filename.csv
		FilePath: w.fileServiceName + fileservice.ServiceNameSeparator + path.Join(w.dir, w.filename) + CsvExtension,
		Entries: []fileservice.IOEntry{
			{
				Offset: int64(w.offset),
				Size:   int64(n),
				Data:   p,
			},
		},
	}); err == nil {
		w.offset += n
	} else if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) && !mkdirTried {
		mkdirTried = true
		goto mkdirRetry
	}
	// XXX Why call this?
	// _ = errors.WithContext(w.ctx, err)
	return
}

// WriteString implement io.StringWriter
func (w *FSWriter) WriteString(s string) (n int, err error) {
	var b = String2Bytes(s)
	return w.Write(b)
}

type FSWriterFactory func(context.Context, string, batchpipe.HasName) io.StringWriter

func GetFSWriterFactory(fs fileservice.FileService, nodeUUID, nodeType string) FSWriterFactory {
	return func(_ context.Context, dir string, i batchpipe.HasName) io.StringWriter {
		return NewFSWriter(DefaultContext(), fs,
			WithPrefix(i),
			WithDir(dir),
			WithNode(nodeUUID, nodeType),
		)
	}
}
