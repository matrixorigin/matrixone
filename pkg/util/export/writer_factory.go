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
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var _ table.RowWriter = (*reactWriter)(nil)
var _ table.AfterWrite = (*reactWriter)(nil)

// reactWriter implement table.AfterWrite, it can react before/after FlushAndClose
type reactWriter struct {
	ctx context.Context
	w   table.RowWriter

	// implement AfterWrite
	afters []table.CheckWriteHook
}

func newWriter(ctx context.Context, w table.RowWriter) *reactWriter {
	return &reactWriter{
		ctx: ctx,
		w:   w,
	}
}

func (rw *reactWriter) WriteRow(row *table.Row) error {
	return rw.w.WriteRow(row)
}

func (rw *reactWriter) GetContent() string {
	return rw.w.GetContent()
}

func (rw *reactWriter) FlushAndClose() (int, error) {
	n, err := rw.w.FlushAndClose()
	if err == nil {
		for _, hook := range rw.afters {
			hook(rw.ctx)
		}
	}
	return n, err
}

func (rw *reactWriter) AddAfter(hook table.CheckWriteHook) {
	rw.afters = append(rw.afters, hook)
}

func GetWriterFactory(fs fileservice.FileService, nodeUUID, nodeType string, enableSqlWriter bool) table.WriterFactory {

	var extension = table.CsvExtension
	var cfg = table.FilePathCfg{NodeUUID: nodeUUID, NodeType: nodeType, Extension: extension}
	var factory func(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter

	switch extension {
	case table.CsvExtension:
		factory = func(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
			options := []etl.FSWriterOption{
				etl.WithFilePath(cfg.LogsFilePathFactory(account, tbl, ts)),
			}
			cw := etl.NewCSVWriter(ctx, etl.NewFSWriter(ctx, fs, options...))
			if enableSqlWriter {
				return newWriter(ctx, etl.NewSqlWriter(ctx, tbl, cw))
			} else {
				return newWriter(ctx, cw)
			}
		}
	case table.TaeExtension:
		// Deprecated
	}

	bufferWriterFactory := func(ctx context.Context, filepath string) io.WriteCloser {
		return etl.NewBufWriter(ctx, etl.NewFSWriter(ctx, fs, etl.WithFilePath(filepath)))
	}

	return table.NewWriterFactoryGetter(factory, bufferWriterFactory)
}
