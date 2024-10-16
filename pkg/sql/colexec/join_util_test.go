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

package colexec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestBatches(t *testing.T) {
	var batches Batches
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
	inputBatch := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(100000), proc.Mp())
	err := batches.CopyIntoBatches(inputBatch, proc)
	inputBatch.Clean(proc.Mp())
	require.NoError(t, err)
	require.Equal(t, 13, len(batches.Buf))
	require.Equal(t, 8192, batches.Buf[0].RowCount())
	require.Equal(t, 8192, batches.Buf[8].RowCount())
	require.Equal(t, 8192, batches.Buf[11].RowCount())
	require.Equal(t, 1696, batches.Buf[12].RowCount())
	batches.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())

	inputBatch = testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(10), proc.Mp())
	err = batches.CopyIntoBatches(inputBatch, proc)
	require.NoError(t, err)
	inputBatch.Clean(proc.Mp())
	inputBatch = testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(8192), proc.Mp())
	err = batches.CopyIntoBatches(inputBatch, proc)
	require.NoError(t, err)
	inputBatch.Clean(proc.Mp())
	inputBatch = testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(10), proc.Mp())
	err = batches.CopyIntoBatches(inputBatch, proc)
	require.NoError(t, err)
	inputBatch.Clean(proc.Mp())
	inputBatch = testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(8192), proc.Mp())
	err = batches.CopyIntoBatches(inputBatch, proc)
	require.NoError(t, err)
	inputBatch.Clean(proc.Mp())
	require.Equal(t, 3, len(batches.Buf))
	require.Equal(t, 8192, batches.Buf[0].RowCount())
	require.Equal(t, 8192, batches.Buf[1].RowCount())
	require.Equal(t, 20, batches.Buf[2].RowCount())
	batches.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}
