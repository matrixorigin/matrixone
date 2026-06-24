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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAnyValueBatchFillReturnsSetRawBytesAtError(t *testing.T) {
	inputMp := mpool.MustNewZero()
	input := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(input, make([]byte, 4096), false, inputMp))
	defer input.Free(inputMp)

	exec, limitedMp, filler := newLimitedAnyValueExec(t)
	defer cleanupLimitedAnyValueExec(exec, limitedMp, filler)

	err := exec.BatchFill(0, []uint64{1}, []*vector.Vector{input})
	require.Error(t, err)
}

func TestAnyValueBatchMergeReturnsSetRawBytesAtError(t *testing.T) {
	inputMp := mpool.MustNewZero()
	input := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(input, make([]byte, 4096), false, inputMp))
	defer input.Free(inputMp)

	sourceMp := mpool.MustNewZero()
	source := makeAnyValueExec(sourceMp, 1, types.T_varchar.ToType()).(*anyExec)
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.BatchFill(0, []uint64{1}, []*vector.Vector{input}))
	defer source.Free()

	target, limitedMp, filler := newLimitedAnyValueExec(t)
	defer cleanupLimitedAnyValueExec(target, limitedMp, filler)

	err := target.BatchMerge(source, 0, []uint64{1})
	require.Error(t, err)
}

func newLimitedAnyValueExec(t *testing.T) (*anyExec, *mpool.MPool, []byte) {
	t.Helper()

	limitedMp, err := mpool.NewMPool("any-value-limited", 1024*1024, mpool.NoFixed)
	require.NoError(t, err)

	exec := makeAnyValueExec(limitedMp, 1, types.T_varchar.ToType()).(*anyExec)
	require.NoError(t, exec.GroupGrow(1))

	remaining := 1024*1024 - limitedMp.CurrNB()
	require.Greater(t, remaining, int64(4096))
	filler, err := limitedMp.Alloc(int(remaining-1024), true)
	require.NoError(t, err)

	return exec, limitedMp, filler
}

func cleanupLimitedAnyValueExec(exec *anyExec, mp *mpool.MPool, filler []byte) {
	if filler != nil {
		mp.Free(filler)
	}
	exec.Free()
	mpool.DeleteMPool(mp)
}
