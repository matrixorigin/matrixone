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

package multi_update

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type partitionIndexTestStorage struct {
	partitionservice.PartitionStorage
	metadata partition.PartitionMetadata
}

func (s *partitionIndexTestStorage) GetMetadata(
	context.Context,
	uint64,
	client.TxnOperator,
) (partition.PartitionMetadata, bool, error) {
	return s.metadata, false, nil
}

type partitionIndexTestService struct {
	partitionservice.PartitionService
	storage partitionservice.PartitionStorage
}

func (s *partitionIndexTestService) GetStorage() partitionservice.PartitionStorage {
	return s.storage
}

func (s *partitionIndexTestService) Enabled() bool {
	return true
}

type capturedIndexBatch struct {
	rows  []int64
	attrs []string
}

func TestPartitionMultiUpdateRoutesIndexOnlyTargetsThroughPhysicalRelations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc(t)
	defer proc.Free()
	proc.Ctx = context.Background()

	const parentID uint64 = 700
	const indexOne uint64 = 901
	const indexTwo uint64 = 902

	expr0 := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}},
	}
	expr1 := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}},
	}
	parentDef := &plan.TableDef{
		TblId:       parentID,
		Version:     9,
		FeatureFlag: features.Partitioned,
		Partition: &plan.Partition{PartitionDefs: []*plan.PartitionDef{
			{Def: expr0},
			{Def: expr1},
		}},
	}
	metadata := partition.PartitionMetadata{
		TableID: parentID,
		Partitions: []partition.Partition{
			{Position: 0, PartitionID: 701, PartitionTableName: "p0", Expr: expr0},
			{Position: 1, PartitionID: 702, PartitionTableName: "p1", Expr: expr1},
		},
	}
	proc.Base.PartitionService = &partitionIndexTestService{
		storage: &partitionIndexTestStorage{metadata: metadata},
	}

	indexDef := func(id uint64, name string) *plan.TableDef {
		return &plan.TableDef{
			TblId:       id,
			Name:        name,
			TableType:   catalog.FullTextIndex_TblType,
			FeatureFlag: features.IndexTable,
			Cols: []*plan.ColDef{
				{ColId: 0, Name: "token", Typ: i64typ},
				{ColId: 1, Name: "pk", Typ: i64typ},
				{ColId: 2, Name: catalog.Row_ID, Typ: rowIdTyp},
			},
		}
	}
	logicalOne := indexDef(indexOne, catalog.FullTextIndexTableNamePrefix+"logical_i1")
	logicalTwo := indexDef(indexTwo, catalog.FullTextIndexTableNamePrefix+"logical_i2")

	makeRelation := func(name string, def *plan.TableDef, extra *api.SchemaExtra) *mock_frontend.MockRelation {
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableName().Return(name).AnyTimes()
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
		rel.EXPECT().GetExtraInfo().Return(extra).AnyTimes()
		rel.EXPECT().Reset(gomock.Any()).Return(nil).AnyTimes()
		return rel
	}

	parentRel := makeRelation("parent", parentDef, &api.SchemaExtra{IndexTables: []uint64{indexOne, indexTwo}})
	physicalOne := makeRelation("p0", &plan.TableDef{TblId: 701, Name: "p0"}, &api.SchemaExtra{IndexTables: []uint64{1001, 1002}})
	physicalTwo := makeRelation("p1", &plan.TableDef{TblId: 702, Name: "p1"}, &api.SchemaExtra{IndexTables: []uint64{2001, 2002}})
	logicalOneRel := makeRelation(logicalOne.Name, logicalOne, nil)
	logicalTwoRel := makeRelation(logicalTwo.Name, logicalTwo, nil)
	physicalIndexDefs := map[uint64]*plan.TableDef{
		1001: indexDef(1001, catalog.FullTextIndexTableNamePrefix+"p0_i1"),
		1002: indexDef(1002, catalog.FullTextIndexTableNamePrefix+"p0_i2"),
		2001: indexDef(2001, catalog.FullTextIndexTableNamePrefix+"p1_i1"),
		2002: indexDef(2002, catalog.FullTextIndexTableNamePrefix+"p1_i2"),
	}
	physicalIndexRels := make(map[uint64]*mock_frontend.MockRelation)
	capturedWrites := make(map[string][]capturedIndexBatch)
	capturedDeletes := make(map[string][]capturedIndexBatch)
	var failWrite bool
	for id, def := range physicalIndexDefs {
		name := def.Name
		rel := makeRelation(name, def, nil)
		rel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, b *batch.Batch) error {
				if failWrite && strings.HasPrefix(name, catalog.FullTextIndexTableNamePrefix+"p1_") {
					return fmt.Errorf("injected physical write failure for %s", name)
				}
				values := append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](b.Vecs[0])...)
				capturedWrites[name] = append(capturedWrites[name], capturedIndexBatch{
					rows:  values,
					attrs: append([]string(nil), b.Attrs...),
				})
				return nil
			},
		).AnyTimes()
		rel.EXPECT().Delete(gomock.Any(), gomock.Any(), catalog.Row_ID).DoAndReturn(
			func(_ context.Context, b *batch.Batch, _ string) error {
				values := append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](b.Vecs[1])...)
				capturedDeletes[name] = append(capturedDeletes[name], capturedIndexBatch{rows: values})
				return nil
			},
		).AnyTimes()
		physicalIndexRels[id] = rel
	}

	byName := map[string]engine.Relation{
		logicalOne.Name:              logicalOneRel,
		logicalTwo.Name:              logicalTwoRel,
		physicalIndexDefs[1001].Name: physicalIndexRels[1001],
		physicalIndexDefs[1002].Name: physicalIndexRels[1002],
		physicalIndexDefs[2001].Name: physicalIndexRels[2001],
		physicalIndexDefs[2002].Name: physicalIndexRels[2002],
	}
	byID := map[uint64]engine.Relation{
		parentID: parentRel,
		701:      physicalOne,
		702:      physicalTwo,
		1001:     physicalIndexRels[1001],
		1002:     physicalIndexRels[1002],
		2001:     physicalIndexRels[2001],
		2002:     physicalIndexRels[2002],
	}

	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string, _ any) (engine.Relation, error) {
			rel, ok := byName[name]
			if !ok {
				return nil, fmt.Errorf("unexpected relation lookup %q", name)
			}
			return rel, nil
		},
	).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{}).AnyTimes()
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ any, id uint64) (string, string, engine.Relation, error) {
			rel, ok := byID[id]
			if !ok {
				return "", "", nil, fmt.Errorf("unexpected relation id %d", id)
			}
			return "test", "db", rel, nil
		},
	).AnyTimes()

	makeCtx := func(id uint64, def *plan.TableDef, name string) *MultiUpdateCtx {
		return &MultiUpdateCtx{
			ObjRef:   &plan.ObjectRef{SchemaName: "test", Obj: int64(id), ObjName: name},
			TableDef: def,
			PartitionIndexCtx: &plan.PartitionIndexCtx{
				ParentRef:    &plan.ObjectRef{SchemaName: "test", Obj: int64(parentID), ObjName: "parent"},
				ParentTable:  parentDef,
				PartitionCol: plan.ColRef{ColPos: 2},
			},
			InsertCols:         []int{1},
			DeleteCols:         []int{0, 1},
			IgnoreAffectedRows: true,
		}
	}
	raw := &MultiUpdate{
		Action: UpdateWriteTable,
		Engine: eng,
		MultiUpdateCtx: []*MultiUpdateCtx{
			makeCtx(indexOne, logicalOne, logicalOne.Name),
			makeCtx(indexTwo, logicalTwo, logicalTwo.Name),
		},
	}
	op := NewPartitionMultiUpdate(raw).(*PartitionMultiUpdate)
	child := colexec.NewMockOperator()
	op.AppendChild(child)

	makeInput := func(pk []int64, routes []int32) *batch.Batch {
		b := batch.New([]string{"row_id", "pk", "partition_route"})
		b.SetVector(0, testutil.MakeRowIdVector([]types.Rowid{
			types.BuildTestRowid(1, pk[0]),
			types.BuildTestRowid(1, pk[1]),
			types.BuildTestRowid(1, pk[2]),
		}, nil, proc.Mp()))
		b.SetVector(1, testutil.MakeInt64Vector(pk, nil, proc.Mp()))
		b.SetVector(2, testutil.NewInt32Vector(len(routes), types.T_int32.ToType(), proc.Mp(), false, nil, routes))
		b.SetRowCount(len(pk))
		return b
	}

	first := makeInput([]int64{11, 22, 33}, []int32{1, 0, 1})
	child.WithBatchs([]*batch.Batch{first})
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Same(t, first, result.Batch)
	require.Equal(t, []int32{1, 0, 1}, vector.MustFixedColNoTypeCheck[int32](first.Vecs[2]))
	require.Equal(t, []int64{11, 22, 33}, vector.MustFixedColNoTypeCheck[int64](first.Vecs[1]))

	for _, name := range []string{physicalIndexDefs[1001].Name, physicalIndexDefs[1002].Name} {
		require.Equal(t, [][]int64{{22}}, capturedRows(capturedWrites[name]))
		require.Equal(t, [][]int64{{22}}, capturedRows(capturedDeletes[name]))
	}
	for _, name := range []string{physicalIndexDefs[2001].Name, physicalIndexDefs[2002].Name} {
		require.Equal(t, [][]int64{{11, 33}}, capturedRows(capturedWrites[name]))
		require.Equal(t, [][]int64{{11, 33}}, capturedRows(capturedDeletes[name]))
	}
	for _, writes := range capturedWrites {
		for _, write := range writes {
			require.NotContains(t, write.attrs, "partition_route")
		}
	}
	require.Zero(t, op.GetAffectedRows(), "index-only physical writes are not client-visible affected rows")

	child.Reset(proc, false, nil)
	child.ResetBatchs()
	op.Reset(proc, false, nil)
	second := makeInput([]int64{44, 55, 66}, []int32{0, 1, 0})
	child.WithBatchs([]*batch.Batch{second})
	require.NoError(t, op.Prepare(proc))
	_, err = op.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1, 0}, vector.MustFixedColNoTypeCheck[int32](second.Vecs[2]))
	require.Equal(t, [][]int64{{22}, {44, 66}}, capturedRows(capturedWrites[physicalIndexDefs[1001].Name]))
	require.Equal(t, [][]int64{{22}, {44, 66}}, capturedRows(capturedWrites[physicalIndexDefs[1002].Name]))
	require.Equal(t, [][]int64{{11, 33}, {55}}, capturedRows(capturedWrites[physicalIndexDefs[2001].Name]))
	require.Equal(t, [][]int64{{11, 33}, {55}}, capturedRows(capturedWrites[physicalIndexDefs[2002].Name]))

	// A later physical partition can fail after the first one has accepted its
	// batch. The wrapper must return the error and release its relation mapping
	// on the failure reset; it must not silently fall back to the logical index.
	failWrite = true
	child.Reset(proc, false, nil)
	child.ResetBatchs()
	op.Reset(proc, false, nil)
	third := makeInput([]int64{77, 88, 99}, []int32{0, 1, 0})
	child.WithBatchs([]*batch.Batch{third})
	require.NoError(t, op.Prepare(proc))
	_, err = op.Call(proc)
	require.Error(t, err)
	require.Equal(t, []int64{77, 88, 99}, vector.MustFixedColNoTypeCheck[int64](third.Vecs[1]))
	require.Equal(t, [][]int64{{22}, {44, 66}, {77, 99}}, capturedRows(capturedWrites[physicalIndexDefs[1001].Name]))
	require.Equal(t, [][]int64{{22}, {44, 66}, {77, 99}}, capturedRows(capturedWrites[physicalIndexDefs[1002].Name]))
	require.Equal(t, [][]int64{{11, 33}, {55}}, capturedRows(capturedWrites[physicalIndexDefs[2001].Name]))
	require.Equal(t, [][]int64{{11, 33}, {55}}, capturedRows(capturedWrites[physicalIndexDefs[2002].Name]))
	op.Reset(proc, true, err)
	require.Empty(t, op.targets[0].partitionIndexes)

	op.Free(proc, false, nil)
	child.Free(proc, false, nil)
	require.Empty(t, op.targets[0].partitionIndexes)
}

func capturedRows(batches []capturedIndexBatch) [][]int64 {
	rows := make([][]int64, 0, len(batches))
	for _, b := range batches {
		rows = append(rows, b.rows)
	}
	return rows
}

func TestPartitionIndexRouteRejectsInvalidAndNullRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ctx := &MultiUpdateCtx{
		TableDef:          &plan.TableDef{TblId: 901, FeatureFlag: features.IndexTable},
		PartitionIndexCtx: &plan.PartitionIndexCtx{PartitionCol: plan.ColRef{ColPos: 0}},
	}
	target := &partitionUpdateTarget{
		indexOnly: true,
		contexts:  []*MultiUpdateCtx{ctx},
		meta: partition.PartitionMetadata{Partitions: []partition.Partition{
			{Position: 0, PartitionID: 1},
			{Position: 1, PartitionID: 2},
		}},
	}
	op := &PartitionMultiUpdate{raw: &MultiUpdate{Action: UpdateWriteTable}}

	cases := []struct {
		name   string
		values []int32
		nulls  []bool
		wantOK bool
	}{
		{name: "negative", values: []int32{-1}, wantOK: false},
		{name: "out of range", values: []int32{2}, wantOK: false},
		{name: "null", values: []int32{0}, nulls: []bool{true}, wantOK: false},
		{name: "empty", values: nil, wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bat := batch.New([]string{"partition_route"})
			bat.SetVector(0, testutil.NewInt32Vector(len(tc.values), types.T_int32.ToType(), proc.Mp(), false, tc.nulls, tc.values))
			bat.SetRowCount(len(tc.values))
			defer bat.Clean(proc.Mp())
			err := op.writePartitionIndexTarget(proc, target, bat)
			if tc.wantOK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

var _ vm.Operator = (*PartitionMultiUpdate)(nil)
