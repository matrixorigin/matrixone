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
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

// This is a component test of the production Call/WriteS3/flush chain. Catalog
// lookup and the input child are fixtures; object writes and reads are real.
// It does not prove receiving-CN execution or transaction rollback/visibility.
func TestPartitionIndexWriteS3CallFailureCancelAndReuse(t *testing.T) {
	for _, mode := range []string{"success", "write failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			_, ctrl, proc := prepareTestCtx(t, true)
			defer proc.Free()
			ctx, cancel := context.WithTimeout(proc.Ctx, 30*time.Second)
			defer cancel()
			proc.Ctx = ctx
			fs := &partitionS3BoundaryFS{FileService: proc.Base.FileService}
			proc.Base.FileService = fs
			injected := errors.New("injected second physical object write failure")
			if mode == "write failure" {
				fs.beforeSecondWrite = func(context.Context) error { return injected }
			} else if mode == "cancel" {
				fs.beforeSecondWrite = func(writeCtx context.Context) error {
					cancel()
					return writeCtx.Err()
				}
			}

			op := preparePartitionS3BoundaryOperator(t, ctrl, proc)
			child := colexec.NewMockOperator()
			op.AppendChild(child)
			defer op.Free(proc, false, nil)
			defer child.Free(proc, false, nil)
			before := proc.Mp().CurrNB()
			input := partitionS3BoundaryInput(proc, 0)
			child.WithBatchs([]*batch.Batch{input})
			require.NoError(t, op.Prepare(proc))
			result, err := op.Call(proc)
			if mode == "success" {
				require.NoError(t, err)
				checkPartitionS3BoundaryOutput(t, proc, fs, result.Batch, 0)
			} else {
				if mode == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
					require.ErrorIs(t, proc.Ctx.Err(), context.Canceled)
				} else {
					require.ErrorIs(t, err, injected)
				}
				require.Nil(t, result.Batch, "a failed flush must not return partial metadata")
			}
			attempts, completed := fs.snapshot()
			require.Len(t, attempts, 2, "both physical writers must reach FileService.Write")
			if mode == "success" {
				require.Len(t, completed, 2)
			} else {
				require.Len(t, completed, 1, "failure must follow a real completed object write")
				_, statErr := fs.FileService.StatFile(context.Background(), attempts[1])
				require.Error(t, statErr, "the rejected object must not exist")
			}
			for _, path := range completed {
				entry, statErr := fs.FileService.StatFile(context.Background(), path)
				require.NoError(t, statErr)
				require.Positive(t, entry.Size)
			}
			require.Zero(t, op.GetAffectedRows())
			oldWriters := append([]*s3WriterDelegate(nil), op.freeWriters...)
			for _, writer := range op.writers {
				oldWriters = append(oldWriters, writer)
			}
			require.Len(t, oldWriters, 2)
			op.Reset(proc, err != nil, err)
			child.Free(proc, err != nil, err)
			require.Empty(t, op.writers)
			require.Empty(t, op.freeWriters)
			require.Zero(t, op.nextWriterID)
			for _, target := range op.targets {
				require.Empty(t, target.partitionIndexes)
				require.Empty(t, target.writerIDs)
				require.Empty(t, target.meta.Partitions)
			}
			for _, writer := range oldWriters {
				require.Nil(t, writer.insertSinkers)
				require.Nil(t, writer.insertFreeLists)
				require.Nil(t, writer.outputBat)
			}
			require.Equal(t, before, proc.Mp().CurrNB(), "reset releases input, grouping and writer buffers")

			// Reset must permit a fresh generation after both successful and failed
			// flushes, including when the prior process context was cancelled.
			freshCtx, freshCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer freshCancel()
			proc.Ctx = freshCtx
			child.WithBatchs([]*batch.Batch{partitionS3BoundaryInput(proc, 100)})
			require.NoError(t, op.Prepare(proc))
			result, err = op.Call(proc)
			require.NoError(t, err)
			checkPartitionS3BoundaryOutput(t, proc, fs, result.Batch, 100)
			_, after := fs.snapshot()
			require.Len(t, after, len(completed)+2)
			op.Reset(proc, false, nil)
			child.Free(proc, false, nil)
			require.Equal(t, before, proc.Mp().CurrNB())
		})
	}
}

type partitionS3BoundaryFS struct {
	fileservice.FileService
	mu                sync.Mutex
	attempts          []string
	completed         []string
	beforeSecondWrite func(context.Context) error
}

func (fs *partitionS3BoundaryFS) Write(ctx context.Context, v fileservice.IOVector) error {
	fs.mu.Lock()
	fs.attempts = append(fs.attempts, v.FilePath)
	second := len(fs.attempts) == 2
	fs.mu.Unlock()
	if second && fs.beforeSecondWrite != nil {
		if err := fs.beforeSecondWrite(ctx); err != nil {
			return err
		}
	}
	if err := fs.FileService.Write(ctx, v); err != nil {
		return err
	}
	fs.mu.Lock()
	fs.completed = append(fs.completed, v.FilePath)
	fs.mu.Unlock()
	return nil
}

func (fs *partitionS3BoundaryFS) snapshot() ([]string, []string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]string(nil), fs.attempts...), append([]string(nil), fs.completed...)
}

func preparePartitionS3BoundaryOperator(t *testing.T, ctrl *gomock.Controller, proc *process.Process) *PartitionMultiUpdate {
	t.Helper()
	exprs := []*plan.Expr{
		{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}},
		{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}},
	}
	parent := &plan.TableDef{TblId: 700, FeatureFlag: features.Partitioned, Partition: &plan.Partition{
		PartitionDefs: []*plan.PartitionDef{{Def: exprs[0]}, {Def: exprs[1]}},
	}}
	proc.Base.PartitionService = &partitionIndexTestService{storage: &partitionIndexTestStorage{
		metadata: partition.PartitionMetadata{TableID: 700, Partitions: []partition.Partition{
			{Position: 0, PartitionID: 701, Expr: exprs[0]},
			{Position: 1, PartitionID: 702, Expr: exprs[1]},
		}},
	}}
	indexDef := func(id uint64) *plan.TableDef {
		// Stored column order/types match pkg/fulltext/plugin/plan/schema.go.
		return &plan.TableDef{TblId: id, Name: fmt.Sprintf("%s%d", catalog.FullTextIndexTableNamePrefix, id),
			TableType: catalog.FullTextIndex_TblType, FeatureFlag: features.IndexTable,
			Cols: []*plan.ColDef{
				{Name: catalog.FullTextIndex_TabCol_Id, Typ: i64typ, Seqnum: 0},
				{Name: catalog.FullTextIndex_TabCol_Position, Typ: i32typ, Seqnum: 1},
				{Name: catalog.FullTextIndex_TabCol_Word, Typ: varcharTyp, Seqnum: 2},
				{Name: catalog.FakePrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}, Seqnum: 3, Primary: true, NotNull: true},
				{Name: catalog.Row_ID, Typ: rowIdTyp, Seqnum: 4},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName}, ClusterBy: &plan.ClusterByDef{Name: "word"},
		}
	}
	logical := indexDef(901)
	rels := make(map[uint64]engine.Relation)
	for id, def := range map[uint64]*plan.TableDef{700: parent, 701: {TblId: 701}, 702: {TblId: 702}, 1001: indexDef(1001), 2001: indexDef(2001)} {
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
		rel.EXPECT().GetTableName().Return(def.Name).AnyTimes()
		indexes := map[uint64][]uint64{700: {901}, 701: {1001}, 702: {2001}}[id]
		rel.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{IndexTables: indexes}).AnyTimes()
		rels[id] = rel
	}
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ any, id uint64) (string, string, engine.Relation, error) {
			rel, ok := rels[id]
			if !ok {
				return "", "", nil, fmt.Errorf("unexpected table id %d", id)
			}
			return "test", "db", rel, nil
		},
	).AnyTimes()
	return NewPartitionMultiUpdate(&MultiUpdate{Action: UpdateWriteS3, IsRemote: true, Engine: eng,
		MultiUpdateCtx: []*MultiUpdateCtx{{
			ObjRef: &plan.ObjectRef{Obj: 901, ObjName: logical.Name}, TableDef: logical,
			InsertCols: []int{0, 1, 2, 3}, IgnoreAffectedRows: true,
			PartitionIndexCtx: &plan.PartitionIndexCtx{
				ParentRef: &plan.ObjectRef{Obj: 700}, ParentTable: parent, PartitionCol: plan.ColRef{ColPos: 4},
			},
		}},
	}).(*PartitionMultiUpdate)
}

func partitionS3BoundaryInput(proc *process.Process, offset int64) *batch.Batch {
	b := batch.New([]string{"doc_id", "pos", "word", catalog.FakePrimaryKeyColName, "partition_route"})
	b.Vecs[0] = testutil.MakeInt64Vector([]int64{11 + offset, 22 + offset, 33 + offset}, nil, proc.Mp())
	b.Vecs[1] = testutil.MakeInt32Vector([]int32{0, 1, 2}, nil, proc.Mp())
	b.Vecs[2] = testutil.MakeVarcharVector([]string{"alpha", "beta", "gamma"}, nil, proc.Mp())
	b.Vecs[3] = testutil.MakeUint64Vector([]uint64{uint64(1 + offset), uint64(2 + offset), uint64(3 + offset)}, nil, proc.Mp())
	b.Vecs[4] = testutil.MakeInt32Vector([]int32{1, 0, 1}, nil, proc.Mp())
	b.SetRowCount(3)
	return b
}

func checkPartitionS3BoundaryOutput(t *testing.T, proc *process.Process, fs fileservice.FileService, out *batch.Batch, offset int64) {
	t.Helper()
	require.NotNil(t, out)
	require.Equal(t, 2, out.RowCount())
	ids := vector.MustFixedColNoTypeCheck[uint64](out.Vecs[1])
	counts := vector.MustFixedColNoTypeCheck[uint64](out.Vecs[2])
	got := make(map[uint64][]int64)
	for i, id := range ids {
		func() {
			require.Equal(t, uint8(actionInsert), vector.GetFixedAtNoTypeCheck[uint8](out.Vecs[0], i))
			meta := batch.NewOffHeapEmpty()
			defer meta.Clean(proc.Mp())
			require.NoError(t, meta.UnmarshalBinaryWithAnyMp(out.Vecs[4].GetBytesAt(i), proc.Mp()))
			require.Equal(t, []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}, meta.Attrs)
			require.Equal(t, 1, meta.RowCount())
			blk := objectio.DecodeBlockInfo(meta.Vecs[0].GetBytesAt(0))
			reader, err := ioutil.NewObjectReader(fs, blk.MetaLocation())
			require.NoError(t, err)
			rows, release, err := reader.LoadColumns(proc.Ctx, []uint16{0, 1, 2, 3}, nil, blk.MetaLocation().ID(), proc.Mp())
			require.NoError(t, err)
			defer release()
			require.Equal(t, counts[i], uint64(rows.RowCount()))
			require.NotContains(t, ids[:i], id, "each physical writer must flush exactly once")
			got[id] = append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](rows.Vecs[0])...)
			for j, docID := range got[id] {
				baseID := docID - offset
				require.Contains(t, []int64{11, 22, 33}, baseID)
				require.Equal(t, int32(baseID/11-1), vector.GetFixedAtNoTypeCheck[int32](rows.Vecs[1], j))
				require.Equal(t, []string{"alpha", "beta", "gamma"}[baseID/11-1], rows.Vecs[2].GetStringAt(j))
				require.Equal(t, uint64(baseID/11+offset), vector.GetFixedAtNoTypeCheck[uint64](rows.Vecs[3], j))
			}
		}()
	}
	require.Equal(t, map[uint64][]int64{1001: {22 + offset}, 2001: {11 + offset, 33 + offset}}, got)
}
