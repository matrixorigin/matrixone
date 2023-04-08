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
	"bytes"
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl/sqlWriter"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

func GetWriterFactory(fs fileservice.FileService, nodeUUID, nodeType string, enableSqlWriter bool) (factory table.WriterFactory) {
	//Todo : Retire the TAEWriter and old configuration
	var extension = table.CsvExtension
	var cfg = table.FilePathCfg{NodeUUID: nodeUUID, NodeType: nodeType, Extension: extension}

	switch extension {
	case table.CsvExtension:
		factory = func(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
			options := []etl.FSWriterOption{
				etl.WithFilePath(cfg.LogsFilePathFactory(account, tbl, ts)),
			}
			if enableSqlWriter {
				sw := sqlWriter.NewSqlWriter(tbl, ctx)
				return etl.NewCSVWriter(ctx, bytes.NewBuffer(nil), etl.NewFSWriter(ctx, fs, options...), sw)
			} else {
				return etl.NewCSVWriter(ctx, bytes.NewBuffer(nil), etl.NewFSWriter(ctx, fs, options...), nil)
			}

		}
	case table.TaeExtension:
		mp, err := mpool.NewMPool("etl_fs_writer", 0, mpool.NoFixed)
		if err != nil {
			panic(err)
		}
		factory = func(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
			filePath := cfg.LogsFilePathFactory(account, tbl, ts)
			return etl.NewTAEWriter(ctx, tbl, mp, filePath, fs)
		}
	}

	return factory
}
