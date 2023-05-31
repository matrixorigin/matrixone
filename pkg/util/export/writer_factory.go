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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var _ table.RowWriter = (*writer)(nil)
var _ table.AfterWrite = (*writer)(nil)

type writer struct {
	ctx context.Context
	w   table.RowWriter

	// implement AfterWrite
	afters []table.CheckWriteHook
}

func newWriter(ctx context.Context, w table.RowWriter) *writer {
	return &writer{
		ctx: ctx,
		w:   w,
	}
}

func (w *writer) WriteRow(row *table.Row) error {
	return w.w.WriteRow(row)
}

func (w *writer) GetContent() string {
	return w.w.GetContent()
}

func (w *writer) FlushAndClose() (int, error) {
	n, err := w.w.FlushAndClose()
	if err == nil {
		for _, hook := range w.afters {
			hook(w.ctx)
		}
	}
	return n, err
}

func (w *writer) AddAfter(hook table.CheckWriteHook) {
	w.afters = append(w.afters, hook)
}

func GetWriterFactory(fs fileservice.FileService, nodeUUID, nodeType string, ext string) (factory table.WriterFactory) {

	var extension = table.GetExtension(ext)
	var cfg = table.FilePathCfg{NodeUUID: nodeUUID, NodeType: nodeType, Extension: extension}

	switch extension {
	case table.CsvExtension:
		factory = func(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
			options := []etl.FSWriterOption{
				etl.WithFilePath(cfg.LogsFilePathFactory(account, tbl, ts)),
			}
			return newWriter(ctx, etl.NewCSVWriter(ctx, etl.NewFSWriter(ctx, fs, options...)))
		}
	case table.TaeExtension:
		mp, err := mpool.NewMPool("etl_fs_writer", 0, mpool.NoFixed)
		if err != nil {
			panic(err)
		}
		factory = func(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
			filePath := cfg.LogsFilePathFactory(account, tbl, ts)
			return newWriter(ctx, etl.NewTAEWriter(ctx, tbl, mp, filePath, fs))
		}
	}

	return factory
}
