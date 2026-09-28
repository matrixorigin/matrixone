// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi_update

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestPartitionIndexS3WritersFlushPhysicalTableIDs(t *testing.T) {
	_, _, proc := prepareTestCtx(t, true)
	defer proc.Free()
	before := proc.Mp().CurrNB()
	raw := &MultiUpdate{Action: UpdateWriteS3, IsRemote: true}
	op := &PartitionMultiUpdate{raw: raw, writers: make(map[uint64]*s3WriterDelegate)}
	defer op.freePartitionWriters(proc)
	target := &partitionUpdateTarget{writerIDs: make(map[uint64]uint64)}
	analyzer := process.NewAnalyzer(0, false, false, "partition-index-s3")
	// Alternate physical targets to catch accidental reuse of the last context.
	for seq, tableID := range []uint64{1001, 2001, 1001} {
		ref, def := getTestSecondaryIndexTable(fmt.Sprintf("%sp%d", catalog.FullTextIndexTableNamePrefix, tableID))
		ref.Obj, def.TblId = int64(tableID), tableID
		raw.MultiUpdateCtx = []*MultiUpdateCtx{{ObjRef: ref, TableDef: def, InsertCols: []int{0, 1}, IgnoreAffectedRows: true}}
		raw.resetMultiUpdateCtxs()
		raw.addAffectedRowsFunc = op.doAddAffectedRows
		writer, err := op.getS3Writer(proc.GetService(), op.writerID(target, tableID))
		require.NoError(t, err)
		input := batch.New([]string{def.Cols[0].Name, def.Cols[1].Name, "partition_route"})
		input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		input.Vecs[2] = vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(fmt.Sprint(seq)), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(seq), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], int32(1), false, proc.Mp()))
		input.SetRowCount(1)
		err = writer.append(proc, analyzer, input)
		input.Clean(proc.Mp())
		require.NoError(t, err)
	}
	require.Len(t, op.writers, 2)
	rows := make(map[uint64]uint64)
	for writer := op.getFlushableS3Writer(); writer != nil; writer = op.getFlushableS3Writer() {
		require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
		ids := vector.MustFixedColNoTypeCheck[uint64](writer.outputBat.Vecs[1])
		counts := vector.MustFixedColNoTypeCheck[uint64](writer.outputBat.Vecs[2])
		for i, id := range ids {
			require.Equal(t, writer.insertBlockRowCount[0], counts[i], "flush must retain the physical row count")
			rows[id] += counts[i]
		}
	}
	require.Equal(t, map[uint64]uint64{1001: 2, 2001: 1}, rows)
	require.Zero(t, op.GetAffectedRows())
	require.Empty(t, op.writers)
	require.Len(t, op.freeWriters, 2)
	op.freePartitionWriters(proc)
	require.Empty(t, op.freeWriters)
	require.Equal(t, before, proc.Mp().CurrNB(), "all writer and flush buffers must be released")
}
