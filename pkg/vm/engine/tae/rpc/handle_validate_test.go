// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/stretchr/testify/require"
)

func TestValidateInsertBatchReturnsErrorForInvalidBatch(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	_, err := validateInsertBatch(ctx, &cmd_util.WriteReq{
		TableID:   1,
		TableName: "t",
	})
	require.ErrorContains(t, err, "INVALID-INSERT-BATCH-NIL-BATCH")

	_, err = validateInsertBatch(ctx, &cmd_util.WriteReq{
		TableID:   1,
		TableName: "t",
		Batch:     batch.NewWithSize(1),
	})
	require.ErrorContains(t, err, "INVALID-INSERT-BATCH-NIL-VEC")

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], 2, false, mp))
	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[1], 1, false, mp))
	_, err = validateInsertBatch(ctx, &cmd_util.WriteReq{
		TableID:   1,
		TableName: "t",
		Batch:     bat,
	})
	require.ErrorContains(t, err, "INVALID-INSERT-BATCH-DIFF-LENGTH")

	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[1], 2, false, mp))
	rows, err := validateInsertBatch(ctx, &cmd_util.WriteReq{
		TableID:   1,
		TableName: "t",
		Batch:     bat,
	})
	require.NoError(t, err)
	require.Equal(t, 2, rows)
}
