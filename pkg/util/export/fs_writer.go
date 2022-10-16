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
	"io"
	"path"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

const etlFileServiceName = "ETL"

var _ stringWriter = (*FSWriter)(nil)

type FSWriter struct {
	ctx context.Context         // New args
	fs  fileservice.FileService // New args
	ts  time.Time               // WithTimestamp
	// pathBuilder
	pathBuilder PathBuilder // WithPathBuilder

	mux sync.Mutex
	// rejoin builder
	account  string // see WithAccount, default: ""
	name     string // see WithName, as filename name, see GetNewFileName
	database string // see WithDatabase, default: ""
	nodeUUID string // see WithNode
	nodeType string // see WithNode
	// filename auto generated
	filename string // see NewFSWriter
	path     string // see NewFSWriter

	fileServiceName string // see WithFileServiceName

	offset int // see Write, should not have size bigger than 2GB
}

type FSWriterOption func(*FSWriter)

func (f FSWriterOption) Apply(w *FSWriter) {
	f(w)
}

func NewFSWriter(ctx context.Context, fs fileservice.FileService, opts ...FSWriterOption) *FSWriter {
	w := &FSWriter{
		ctx:         ctx,
		fs:          fs,
		ts:          time.Now(),
		pathBuilder: NewDBTablePathBuilder(),
		name:        "info",
		database:    "",
		nodeUUID:    "0",
		nodeType:    "standalone",

		fileServiceName: etlFileServiceName,
	}
	for _, o := range opts {
		o.Apply(w)
	}
	w.filename = w.pathBuilder.NewLogFilename(w.name, w.nodeUUID, w.nodeType, w.ts)
	w.path = w.pathBuilder.Build(w.account, MergeLogTypeLog, w.ts, w.database, w.name)
	return w
}

func WithAccount(a string) FSWriterOption {
	return FSWriterOption(func(w *FSWriter) {
		w.account = a
	})
}

func WithName(item batchpipe.HasName) FSWriterOption {
	return FSWriterOption(func(w *FSWriter) {
		w.name = item.GetName()
	})
}

func WithDatabase(dir string) FSWriterOption {
	return FSWriterOption(func(w *FSWriter) {
		w.database = dir
	})
}
func WithNode(uuid, nodeType string) FSWriterOption {
	return FSWriterOption(func(w *FSWriter) {
		w.nodeUUID, w.nodeType = uuid, nodeType
	})
}

func WithFileServiceName(serviceName string) FSWriterOption {
	return FSWriterOption(func(w *FSWriter) {
		w.fileServiceName = serviceName
	})
}

func WithTimestamp(ts time.Time) FSWriterOption {
	return FSWriterOption(func(w *FSWriter) {
		w.ts = ts
	})
}
func WithPathBuilder(builder PathBuilder) FSWriterOption {
	return FSWriterOption(func(w *FSWriter) {
		w.pathBuilder = builder
	})
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
		FilePath: w.fileServiceName + fileservice.ServiceNameSeparator + path.Join(w.path, w.filename),
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

type FSWriterFactory func(ctx context.Context, db string, name batchpipe.HasName, options ...FSWriterOption) io.StringWriter

func GetFSWriterFactory(fs fileservice.FileService, nodeUUID, nodeType string) FSWriterFactory {
	return func(_ context.Context, db string, name batchpipe.HasName, options ...FSWriterOption) io.StringWriter {
		options = append(options, WithDatabase(db), WithName(name), WithNode(nodeUUID, nodeType))
		return NewFSWriter(DefaultContext(), fs, options...)
	}
}
