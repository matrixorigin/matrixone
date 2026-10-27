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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFlushMetadataBorrowsEncodedPayload(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		for _, action := range []actionType{actionInsert, actionDelete} {
			t.Run(fmt.Sprintf("controlled=%t/action=%d", controlled, action), func(t *testing.T) {
				contexts := []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}, InsertCols: []int{0}}}
				idx, name, rows := 0, "", uint64(17)
				if action == actionDelete {
					contexts = append(contexts, &MultiUpdateCtx{TableDef: &plan.TableDef{TblId: 100}, DeleteCols: []int{0, 1}})
					idx, name, rows = 1, objectio.PhysicalAddr_Attr, 2
				}
				proc, w := metadataProtocolWriter(t, contexts, controlled)
				source := batch.NewOffHeapWithSize(2)
				t.Cleanup(func() { source.Clean(proc.Mp()) })
				if action == actionDelete {
					source.Attrs = DeleteBatchAttrs
					source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_Rowid.ToType())
					block := types.BuildTestBlockid(1, 1)
					for row := 0; row < 2; row++ {
						require.NoError(t, vector.AppendFixed(source.Vecs[0], types.NewRowid(&block, uint32(row)), false, proc.Mp()))
					}
					source.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("x", 1<<20), ""}, []uint64{1}, proc.Mp())
					source.SetRowCount(2)
				} else {
					source.Attrs = []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
					source.Vecs[0] = testutil.MakeTextVector([]string{string(objectio.EncodeBlockInfo(&objectio.BlockInfo{}))}, nil, proc.Mp())
					source.Vecs[1] = vector.NewOffHeapVecWithType(types.T_binary.ToType())
					require.NoError(t, vector.AppendBytes(source.Vecs[1], objectio.ZeroObjectStats[:], false, proc.Mp()))
					source.SetRowCount(1)
				}
				require.NoError(t, w.spoolFrame(proc, action, idx, rows, name, source))
				require.NoError(t, w.readMetadataFrame(proc))
				frame := w.outputBat
				require.Len(t, frame.Vecs, 5)
				require.Equal(t, 1, frame.RowCount())
				tableID := contexts[idx].TableDef.TblId
				require.Equal(t, uint8(action), vector.MustFixedColNoTypeCheck[uint8](frame.Vecs[0])[0])
				require.Equal(t, tableID, vector.MustFixedColNoTypeCheck[uint64](frame.Vecs[1])[0])
				if controlled {
					fixed, err := colexec.InsertMetadataFrameLimit(1, len(contexts))
					require.NoError(t, err)
					if action == actionDelete {
						require.Greater(t, frame.MarshalBinarySize(), fixed)
						require.Equal(t, w.outputLease.Capacity()/4, w.outputFrameLimit)
					} else {
						require.Equal(t, fixed, w.outputFrameLimit)
					}
				}
				payload := frame.Vecs[4].GetBytesAt(0)
				start := uintptr(unsafe.Pointer(unsafe.SliceData(payload)))
				inside := func(ptr uintptr, size int) {
					if size > 0 {
						require.GreaterOrEqual(t, ptr, start)
						require.LessOrEqual(t, ptr+uintptr(size), start+uintptr(len(payload)))
					}
				}
				observed := false
				inspect := func(decoded *batch.Batch) {
					observed = true
					require.Len(t, decoded.Vecs, 2)
					require.Equal(t, source.MarshalBinarySize(), decoded.MarshalBinarySize())
					for col, vec := range decoded.Vecs {
						inside(uintptr(unsafe.Pointer(unsafe.SliceData(vec.GetData()))), len(vec.GetData()))
						inside(uintptr(unsafe.Pointer(unsafe.SliceData(vec.GetArea()))), len(vec.GetArea()))
						ptr, size := vec.GetNulls().RawPtrLen()
						inside(ptr, int((size+63)/64*8))
						require.True(t, vec.NeedDup())
						before, err := source.Vecs[col].MarshalBinary()
						require.NoError(t, err)
						after, err := vec.MarshalBinary()
						require.NoError(t, err)
						require.Equal(t, before, after)
					}
				}
				ctrl := gomock.NewController(t)
				eng, rel := mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockRelation(ctrl)
				eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), tableID).Return("test", "metadata_t", rel, nil).Times(1)
				rel.EXPECT().GetTableName().Return("metadata_t").AnyTimes()
				if action == actionDelete {
					rel.EXPECT().Delete(gomock.Any(), gomock.Any(), name).DoAndReturn(func(_ context.Context, b *batch.Batch, _ string) error { inspect(b); return nil }).Times(1)
				} else {
					rel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, b *batch.Batch) error { inspect(b); return nil }).Times(1)
				}
				op := NewArgument()
				t.Cleanup(func() { op.Free(proc, false, nil); op.Release() })
				op.Action, op.Engine = UpdateFlushS3Info, eng
				op.ctr.sources, op.ctr.action = make(map[uint64]engine.Relation), action
				op.OpAnalyzer = process.NewAnalyzer(0, false, false, "flush-metadata")
				var affected uint64
				op.addAffectedRowsFunc = func(n uint64) { affected += n }
				child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{frame})
				t.Cleanup(func() { child.ResetBatchs(); child.Free(proc, false, nil) })
				op.AppendChild(child)
				before := proc.Mp().AllocatedBytes()
				result, err := op.Call(proc)
				require.NoError(t, err)
				require.Same(t, frame, result.Batch)
				require.True(t, observed)
				require.Equal(t, rows, affected)
				require.Equal(t, before, proc.Mp().AllocatedBytes(), "the engine borrows the admitted frame without another payload allocation")
				require.Len(t, op.ctr.sources, 1)
			})
		}
	}
}

func TestMultiUpdateMetadataBoundAndOverflow(t *testing.T) {
	small, ok := MetadataOutputBound(8<<10, 2, 10)
	require.True(t, ok)
	large, ok := MetadataOutputBound(4<<20, 2, 10)
	require.True(t, ok)
	require.Equal(t, small.ScratchBytes, large.ScratchBytes, "no-copy decoding needs no extra full-payload clone allowance")
	require.Equal(t, 1, large.Rows)
	require.Equal(t, int64(4<<20), large.BatchBytes)
	require.Equal(t, [3]int64{1, 8, 8}, [3]int64{large.ColumnBytes[0], large.ColumnBytes[1], large.ColumnBytes[2]})
	moreTables, ok := MetadataOutputBound(4<<20, 65, 130)
	require.True(t, ok)
	require.Greater(t, moreTables.ScratchBytes, large.ScratchBytes)
	for _, args := range [][3]int64{{0, 1, 1}, {-1, 1, 1}, {8 << 10, 0, 0}, {8 << 10, -1, 1}, {8 << 10, 2, 1}, {8 << 10, math.MaxInt, math.MaxInt}} {
		_, ok := MetadataOutputBound(args[0], int(args[1]), int(args[2]))
		require.False(t, ok)
	}
}

func metadataProtocolWriter(t *testing.T, contexts []*MultiUpdateCtx, controlled bool) (*process.Process, *s3WriterDelegate) {
	t.Helper()
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	if controlled {
		owner := rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(1<<30))
		ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(proc.Ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: 1,
			Inputs: []rscthrottler.InputSlotSpec{{Name: "csv"}, {Name: "output", Count: 1, Bytes: 64 << 20}}, IOBytes: 1}})
		require.NoError(t, err)
		t.Cleanup(ws.Close)
		proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: 1, OutputBytes: 64 << 20}
	}
	w := &s3WriterDelegate{updateCtxs: contexts}
	t.Cleanup(func() { require.NoError(t, w.free(proc)) })
	return proc, w
}

func TestMultiUpdateInsertOnlyChecksEveryContext(t *testing.T) {
	insert := &MultiUpdateCtx{InsertCols: []int{0}}
	remove := &MultiUpdateCtx{DeleteCols: []int{0, 1}}
	require.True(t, InsertOnlyOutput([]*MultiUpdateCtx{insert, insert}))
	for _, contexts := range [][]*MultiUpdateCtx{nil, {}, {nil}, {insert, nil}, {insert, remove}, {remove, insert}} {
		require.False(t, InsertOnlyOutput(contexts))
	}
}

func TestMultiUpdateInsertMetadataRejectsGlobalSlotSizedFrame(t *testing.T) {
	proc, w := metadataProtocolWriter(t, []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}, InsertCols: []int{0}}}, true)
	require.NoError(t, w.ensureOutputSlot(proc))
	limit, err := colexec.InsertMetadataFrameLimit(1, 1)
	require.NoError(t, err)
	require.Equal(t, limit, w.outputFrameLimit)
	require.Less(t, limit+1, w.outputLease.Capacity()/4)
	w.metadataSpool, err = os.CreateTemp(t.TempDir(), "oversized-fixed-metadata-*")
	require.NoError(t, err)
	require.NoError(t, os.Remove(w.metadataSpool.Name()))
	var header [9]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(limit+1))
	header[8] = byte(actionInsert)
	_, err = w.metadataSpool.Write(header[:])
	require.NoError(t, err)
	w.metadataWritten = limit + 1 + 9
	require.NoError(t, w.metadataSpool.Truncate(w.metadataWritten))
	require.ErrorContains(t, w.readMetadataFrame(proc), "exceeds admitted output capacity")
	require.Zero(t, w.metadataPool().AllocatedBytes(), "reject oversized frame before decoding its backing")
}

func TestMultiUpdateMetadataLimitFollowsGrowth(t *testing.T) {
	for _, deletes := range []bool{false, true} {
		ctx := &MultiUpdateCtx{TableDef: &plan.TableDef{TblId: 99}, InsertCols: []int{0}}
		if deletes {
			ctx.DeleteCols = []int{0, 1}
		}
		proc, w := metadataProtocolWriter(t, []*MultiUpdateCtx{ctx}, true)
		require.NoError(t, w.ensureOutputSlot(proc))
		before := w.outputFrameLimit
		require.NoError(t, proc.Base.ImportMemory.WorkingSet.TryGrow(proc.Ctx, rscthrottler.WorkingSetSpec{UnitBytes: 1,
			Inputs: []rscthrottler.InputSlotSpec{{Name: "csv"}, {Name: "output", Count: 1, Bytes: 128 << 20}}, IOBytes: 1}))
		require.NoError(t, w.ensureOutputSlot(proc))
		if deletes {
			require.Equal(t, 2*before, w.outputFrameLimit, "raw delete metadata follows the enlarged reservation")
			require.NoError(t, w.checkMetadataFrameSize(before+1))
		} else {
			require.Equal(t, before, w.outputFrameLimit, "INSERT protocol frames remain unit bounded")
		}
		require.ErrorContains(t, w.checkMetadataFrameSize(w.outputFrameLimit+1), "exceeds admitted output capacity")
	}
}
