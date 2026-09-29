// Copyright 2026 Matrix Origin
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
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// A parquet shard canceled by a sibling's failure must report a cancellation
// the scheduler can resolve to that failure (CI flake in load_data_parquet:
// "internal error: convert go error to mo error reading magic footer of
// parquet file: context canceled" was reported instead of the sibling's
// "column name not found").
func TestParquetOpenFileCanceledBySibling(t *testing.T) {
	data := parquetCoverageBytes(t)
	localPath := filepath.Join(t.TempDir(), "data.parquet")
	require.NoError(t, os.WriteFile(localPath, data, 0644))

	ctx, cancel := context.WithCancelCause(context.Background())
	siblingErr := moerr.NewInvalidInputNoCtx("column name not found")
	cancel(siblingErr)

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      ctx,
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INFILE}},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1, Filepath: localPath}},
	}
	var h ParquetHandler
	err := h.openFile(param, false)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled, "got %v", err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrContextCanceled), "got %v", err)
}
