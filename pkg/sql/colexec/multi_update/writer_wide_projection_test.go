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

package multi_update

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCommittedDeleteWorkspaceSerializesWideProjections(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
	const unit, width = 16 << 10, 64 << 10
	schema := []types.Type{types.T_Rowid.ToType(), types.T_varchar.ToType()}
	ioBytes, err := ioutil.IOEnvelope([][]types.Type{schema, schema}, unit, width+types.RowidSize+types.VarlenaSize)
	require.NoError(t, err)
	outputBytes, err := OutputEnvelope(unit, width+types.RowidSize+types.VarlenaSize, 2)
	require.NoError(t, err)
	owner := rscthrottler.NewMemThrottler("wide-delete-working-set", 1, rscthrottler.WithConstLimit(1<<30))
	ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(proc.Ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: unit,
		Inputs: []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 4 * width}, {Name: "output", Count: 1, Bytes: outputBytes}}, IOBytes: ioBytes}})
	require.NoError(t, err)
	t.Cleanup(ws.Close)
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: unit, SourceBytes: 4 * width, OutputBytes: outputBytes, IOBytes: ioBytes}
	contexts := prepareTestUpdateMultiUpdateCtx(false, false)
	contexts[0].InsertCols = nil
	contexts[0].DeleteCols = []int{0, 1}
	contexts[0].TableDef.TblId = 99
	contexts[0].TableDef.Cols[0].Typ.Id = int32(types.T_varchar)
	second := contexts[0].clone()
	second.TableDef.Name, second.TableDef.TblId = "other_wide_projection", 100
	second.ObjRef.ObjName = second.TableDef.Name
	second.DeleteCols = []int{2, 3}
	contexts = append(contexts, second)
	tc := buildTestCase(contexts, prepareTestEng(ctrl, false), nil, 0, UpdateWriteS3, false)
	t.Cleanup(func() { tc.op.Free(proc, false, nil) })
	require.NoError(t, tc.op.Prepare(proc))
	w := tc.op.ctr.s3Writer
	deny := &admissionThrottler{}
	deny.beforeWait = func(*admissionThrottler) { t.Fatal("a borrowed wide row cannot wait for ordinary D") }
	w.memController.throttler = deny
	var observed int
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().DoAndReturn(func() txn.TxnMeta {
		observed++
		for _, pending := range w.deleteBatches {
			if pending != nil {
				require.Zero(t, pending.Length(), "preparation owns one source projection; future table clones must not coexist in W")
			}
		}
		return txn.TxnMeta{ID: []byte{1}}
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp
	input := batch.NewWithSize(4)
	for table := 0; table < 2; table++ {
		part := committedDeleteInput(t, proc, table+1, 1)
		part.Vecs[1].Free(proc.Mp())
		part.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("x", width)}, nil, proc.Mp())
		copy(input.Vecs[table*2:], part.Vecs)
	}
	// The two fixture batches own these vectors; input only borrows them.
	input.SetRowCount(1)
	require.NoError(t, w.append(proc, tc.op.OpAnalyzer, input))
	require.Equal(t, 2, observed)
	require.Zero(t, deny.granted)
	require.Zero(t, w.chargedBytes())
	rows := make(map[uint64]uint64)
	for {
		require.NoError(t, w.flushTailAndWriteToOutput(proc, tc.op.OpAnalyzer))
		if w.outputBat == nil {
			break
		}
		tables := vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[1])
		counts := vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2])
		for i, table := range tables {
			rows[table] += counts[i]
		}
	}
	require.Equal(t, map[uint64]uint64{99: 1, 100: 1}, rows)
}
