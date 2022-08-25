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
	"github.com/matrixorigin/matrixone/pkg/util/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

const csvExtension = ".csv"

var _ stringWriter = (*FSWriter)(nil)

var _, _ = getLocalFS()

func getLocalFS() (fileservice.FileService, error) {
	//dir := t.TempDir()
	dir := "path_to_file"
	fs, err := fileservice.NewLocalFS("test", dir, 0)
	return fs, err
}

type FSWriter struct {
	ctx context.Context         // New args
	fs  fileservice.FileService // New args

	mux      sync.Mutex
	prefix   string // see WithPrefix, as filename prefix, see GetNewFileName
	dir      string // see WithDir, default: ""
	nodeUUID string // see WithNode
	nodeType string // see WithNode
	filename string // see GetNewFileName, auto generated

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
		nodeUUID: "00000000-0000-0000-0000-000000000000",
		nodeType: "standalone",
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

func (w *FSWriter) GetNewFileName(ts time.Time) string {
	return fmt.Sprintf(`%s_%s_%s_%s`, w.prefix, w.nodeUUID, w.nodeType, ts.Format("20060102.150405.000000"))
}

// Write implement io.Writer, Please execute in series
func (w *FSWriter) Write(p []byte) (n int, err error) {
	w.mux.Lock()
	defer w.mux.Unlock()
	n = len(p)
	if err = w.fs.Write(w.ctx, fileservice.IOVector{
		FilePath: path.Join(w.dir, w.filename) + csvExtension,
		Entries: []fileservice.IOEntry{
			{
				Offset: w.offset,
				Size:   n,
				Data:   p,
			},
		},
	}); err != nil {
		w.offset += n
	}
	_ = errors.WithContext(w.ctx, err)
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

type FSConfig struct {
	// base FileService config
	backend string
	baseDir string
	// for S3
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	bucket          string
	region          string
}

func (c *FSConfig) Backend() string {
	return c.backend
}

func (c *FSConfig) BaseDir() string {
	return c.baseDir
}

func (c *FSConfig) Endpoint() string {
	return c.endpoint
}

func (c *FSConfig) AccessKeyID() string {
	return c.accessKeyID
}

func (c *FSConfig) SecretAccessKey() string {
	return c.secretAccessKey
}

func (c *FSConfig) Bucket() string {
	return c.bucket
}

func (c *FSConfig) Region() string {
	return c.region
}

const (
	// fixme: just use fileservice/config.go const value

	MemFSBackend  = "MEM"
	DiskFSBackend = "DISK"
	S3FSBackend   = "S3"
)

func ParseFileService(ctx context.Context, fsConfig fileservice.Config) (fs fileservice.FileService, cfg FSConfig, err error) {
	switch fsConfig.Backend {
	case DiskFSBackend, MemFSBackend:
		if fs, err = fileservice.NewLocalETLFS(fsConfig.Name+"ETL", fsConfig.DataDir); err != nil {
			err = moerr.NewPanicError(err)
			return
		}
		cfg.backend = DiskFSBackend
		cfg.baseDir = fsConfig.DataDir
	default:
		if fs, err = fileservice.NewFileService(fsConfig); err != nil {
			err = moerr.NewPanicError(err)
			return
		}
		cfg.backend = S3FSBackend
		cfg.baseDir = fsConfig.S3.KeyPrefix
		cfg.endpoint = fsConfig.S3.Endpoint
		cfg.bucket = fsConfig.S3.Bucket
		var S3Cfg aws.Config
		var credentials aws.Credentials
		if S3Cfg, err = config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(fsConfig.S3.SharedConfigProfile)); err != nil {
			err = moerr.NewPanicError(err)
			return
		} else if credentials, err = S3Cfg.Credentials.Retrieve(ctx); err != nil {
			err = moerr.NewPanicError(err)
			return
		}
		cfg.accessKeyID = credentials.AccessKeyID
		cfg.secretAccessKey = credentials.SecretAccessKey
		cfg.region = S3Cfg.Region
	}
	return
}
