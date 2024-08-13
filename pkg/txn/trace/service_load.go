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

package trace

import (
	"context"
	"fmt"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

func (s *service) writeToMO(
	e loadAction,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			res, err := txn.Exec(e.sql, executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			return nil
		},
		executor.Options{}.
			WithDatabase(DebugDB).
			WithDisableTrace())
}

func (s *service) writeToS3(
	e loadAction,
) error {
	f, err := os.Open(e.file)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	s3 := filepath.Join(catalog.TraceDir, e.file[len(s.dir):])
	writeVec := fileservice.IOVector{
		FilePath: s3,
		Entries: []fileservice.IOEntry{
			{
				Offset:         0,
				ReaderForWrite: f,
				Size:           -1,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	defer cancel()

	s.logger.Info("write trace to s3",
		zap.String("table", e.table),
		zap.String("s3", s3),
		zap.String("size", getFileSize(stat.Size())),
	)
	v2.FSWriteTraceCounter.Add(1)
	return s.options.fs.Write(ctx, writeVec)
}

func getFileSize(value int64) string {
	if value < 1024 {
		return fmt.Sprintf("%d bytes", value)
	} else if value < 1024*1024 {
		return fmt.Sprintf("%d KB", value/1024)
	} else {
		return fmt.Sprintf("%d MB", value/1024/1024)
	}
}
