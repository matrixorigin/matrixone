// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestResultCloneBytesAccountsForSharedPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	const rows = 128
	const limit = 4 << 20
	constant, err := vector.NewConstBytes(types.T_varchar.ToType(), bytes.Repeat([]byte{'x'}, 128<<10), rows, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	constantBatch := batch.NewWithSize(1)
	constantBatch.Vecs[0] = constant
	constantBatch.SetRowCount(rows)
	compact, err := cloneInternalExecutorResultBatch(constantBatch, mp, false)
	require.NoError(t, err)
	defer compact.Clean(mp)
	compactBytes, err := resultCloneBytes(constantBatch, 0, limit)
	require.NoError(t, err, "a compact constant does not duplicate its payload")
	require.GreaterOrEqual(t, compactBytes, uint64(compact.Size()))
	_, err = resultCloneBytes(constantBatch, limit-compactBytes+1, limit)
	require.Error(t, err, "the limit also accounts for earlier result batches")
	flat := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, flat.UnionBatch(constant, 0, rows, nil, mp))
	source := batch.NewWithSize(1)
	source.Vecs[0] = flat
	source.SetRowCount(rows)
	defer source.Clean(mp)
	require.Less(t, source.Size(), limit)

	// The same clone used by the executor materializes the shared payload for
	// each row; the original batch's physical area is not a sufficient bound.
	cloned, err := cloneInternalExecutorResultBatch(source, mp, false)
	require.NoError(t, err)
	defer cloned.Clean(mp)
	require.Greater(t, cloned.Size(), limit)
	_, err = resultCloneBytes(source, 0, limit)
	var limitErr *executor.ResultLimitError
	require.ErrorAs(t, err, &limitErr)
	require.EqualValues(t, limit, limitErr.Limit)
	measured, err := resultCloneBytes(source, 0, uint64(cloned.Size()))
	require.NoError(t, err)
	require.GreaterOrEqual(t, measured, uint64(cloned.Size()))

	// Null rows retain stale descriptors in the source but clone no payload.
	flat.SetNull(0)
	nullClone, err := cloneInternalExecutorResultBatch(source, mp, false)
	require.NoError(t, err)
	defer nullClone.Clean(mp)
	measured, err = resultCloneBytes(source, 0, uint64(nullClone.Size()))
	require.NoError(t, err)
	require.GreaterOrEqual(t, measured, uint64(nullClone.Size()))

	sources := make([]types.StringSource, rows)
	kinds := make([]vector.PrepareParamKind, rows)
	binary := make([]bool, rows)
	for i := 0; i < rows; i += 2 {
		sources[i] = types.StringSourceLiteral
		kinds[i] = vector.PrepareParamInteger
		binary[i] = true
	}
	require.NoError(t, flat.SetStringSourcesWithMP(sources, mp))
	require.NoError(t, flat.SetPrepareParamKindsWithMP(kinds, mp))
	require.NoError(t, flat.SetBinaryStringRowsWithMP(binary, mp))
	metadataClone, err := cloneInternalExecutorResultBatch(source, mp, false)
	require.NoError(t, err)
	defer metadataClone.Clean(mp)
	measured, err = resultCloneBytes(source, 0, ^uint64(0))
	require.NoError(t, err)
	require.GreaterOrEqual(t, measured, uint64(metadataClone.Size()), "mixed metadata must also fit the bound")

	require.NoError(t, constant.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
	constantMetadataClone, err := cloneInternalExecutorResultBatch(constantBatch, mp, false)
	require.NoError(t, err)
	defer constantMetadataClone.Clean(mp)
	measured, err = resultCloneBytes(constantBatch, 0, limit)
	require.NoError(t, err)
	require.GreaterOrEqual(t, measured, uint64(constantMetadataClone.Size()), "constant metadata must fit the bound")

	// Reject corrupt references before cloning can slice outside the area.
	values, area := vector.MustVarlenaRawData(flat)
	values[1].SetOffsetLen(uint32(len(area)), 1)
	_, err = resultCloneBytes(source, 0, ^uint64(0))
	require.ErrorContains(t, err, "invalid varlen result payload")
}

func TestResultCloneBytesBorrowedWindowAndOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	parent := vector.NewVec(types.T_varchar.ToType())
	defer parent.Free(mp)
	payload := bytes.Repeat([]byte{'x'}, 128<<10)
	for i := 0; i < 40; i++ {
		require.NoError(t, vector.AppendBytes(parent, payload, false, mp))
	}
	window, err := parent.Window(0, 1)
	require.NoError(t, err)
	defer window.Free(mp)
	input := batch.NewWithSize(1)
	input.Vecs[0] = window
	input.SetRowCount(1)
	cloned, err := cloneInternalExecutorResultBatch(input, mp, false)
	require.NoError(t, err)
	defer cloned.Clean(mp)
	measured, err := resultCloneBytes(input, 0, 4<<20)
	require.NoError(t, err, "a small window must not be charged for its parent's full area")
	require.GreaterOrEqual(t, measured, uint64(cloned.Size()))

	huge := vector.NewConstNull(types.T_int64.ToType(), 1, mp)
	defer huge.Free(mp)
	huge.SetLength(int(^uint(0) >> 1))
	input.Vecs[0] = huge
	_, err = resultCloneBytes(input, 0, ^uint64(0))
	require.Error(t, err, "an unrepresentable materialized row area must fail before allocation")
}
