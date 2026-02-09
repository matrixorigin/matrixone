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

package external

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ParquetReader handles Parquet format files.
// Phase 1: thin wrapper around existing ParquetHandler logic.
type ParquetReader struct {
	param *ExternalParam
	h     *ParquetHandler
}

func NewParquetReader(param *ExternalParam, proc *process.Process) *ParquetReader {
	return &ParquetReader{}
}

func (r *ParquetReader) Open(param *ExternalParam, proc *process.Process) (fileEmpty bool, err error) {
	r.param = param
	r.h, err = newParquetHandler(param)
	if err != nil {
		return false, err
	}
	// newParquetHandler returns (nil, nil) for empty files
	if r.h == nil {
		return true, nil
	}
	return false, nil
}

func (r *ParquetReader) ReadBatch(
	ctx context.Context, buf *batch.Batch,
	proc *process.Process, analyzer process.Analyzer,
) (fileFinished bool, err error) {
	_, span := trace.Start(ctx, "ParquetReader.ReadBatch")
	defer span.End()

	if r.h == nil {
		return true, nil
	}

	r.h.batchCnt = maxParquetBatchCnt

	err = r.h.getData(buf, r.param, proc)
	if err != nil {
		return false, err
	}

	// Check if file is finished: getData sets offset and checks NumRows
	if r.h.file != nil && r.h.offset >= r.h.file.NumRows() {
		return true, nil
	}
	return false, nil
}

func (r *ParquetReader) Close() error {
	if r.h != nil {
		r.h.cleanup() // close rowReader (nested column path)
		for i, pages := range r.h.pages {
			if pages != nil {
				pages.Close()
				r.h.pages[i] = nil
			}
		}
		r.h = nil
	}
	r.param = nil
	return nil
}
