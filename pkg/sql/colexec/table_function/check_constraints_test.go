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

package table_function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestAppendEncodedCheckConstraintRowsDecodesSchemaExtra(t *testing.T) {
	rows := make([]checkConstraintRow, 0)
	data := api.MustMarshalTblExtra(&api.SchemaExtra{
		Checks: []*planpb.CheckDef{{
			Name:      "amount_positive",
			OriginSql: "`amount` > 0",
		}},
	})

	require.NoError(t, appendEncodedCheckConstraintRows(&rows, "app", data))
	require.Equal(t, []checkConstraintRow{
		{schema: "app", name: "amount_positive", clause: "`amount` > 0"},
	}, rows)
}

func TestAppendEncodedCheckConstraintRowsRejectsMalformedMetadata(t *testing.T) {
	rows := make([]checkConstraintRow, 0)
	require.Error(t, appendEncodedCheckConstraintRows(&rows, "app", []byte{0xff}))
	require.Empty(t, rows)
}

func TestAppendCheckConstraintRowsDecodesCheckDef(t *testing.T) {
	rows := make([]checkConstraintRow, 0)
	appendCheckConstraintRows(&rows, "app", []*planpb.CheckDef{
		{Name: "amount_positive", OriginSql: "`amount` > 0"},
		{Name: "status_valid", OriginSql: "`status` in ('new','done')"},
	})

	require.Equal(t, []checkConstraintRow{
		{schema: "app", name: "amount_positive", clause: "`amount` > 0"},
		{schema: "app", name: "status_valid", clause: "`status` in ('new','done')"},
	}, rows)
}

func TestAppendCheckConstraintRowsHandlesEmptyChecks(t *testing.T) {
	rows := make([]checkConstraintRow, 0)
	// Temporary and internal relation filtering is performed by the catalog
	// predicate in collectCheckConstraintRows; this helper only decodes rows.
	appendCheckConstraintRows(&rows, "app", nil)
	require.Empty(t, rows)
}

func TestCheckConstraintOutputPositionsAllowPrunedColumns(t *testing.T) {
	require.Equal(t,
		[4]int{checkConstraintCatalogColumn, checkConstraintSchemaColumn, checkConstraintNameColumn, -1},
		checkConstraintOutputPositions([]string{
			"constraint_catalog",
			"constraint_schema",
			"constraint_name",
		}))
	require.Equal(t,
		[4]int{-1, 0, 1, 2},
		checkConstraintOutputPositions([]string{
			"CONSTRAINT_SCHEMA",
			"CONSTRAINT_NAME",
			"CHECK_CLAUSE",
		}))
}

func TestAppendCheckConstraintRowAllowsPrunedColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer proc.Free()

	vectors := []*vector.Vector{
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
	}
	for _, vec := range vectors {
		defer vec.Free(mp)
	}

	positions := checkConstraintOutputPositions([]string{
		"constraint_catalog",
		"constraint_schema",
		"constraint_name",
	})
	require.NoError(t, appendCheckConstraintRow(vectors, positions, checkConstraintRow{
		schema: "app",
		name:   "amount_positive",
		clause: "`amount` > 0",
	}, proc))
	require.Equal(t, "def", vectors[0].GetStringAt(0))
	require.Equal(t, "app", vectors[1].GetStringAt(0))
	require.Equal(t, "amount_positive", vectors[2].GetStringAt(0))
}

func TestAppendCheckConstraintRowRejectsUnavailableVector(t *testing.T) {
	proc := testutil.NewProc(t)
	positions := [4]int{0, -1, -1, -1}
	require.Error(t, appendCheckConstraintRow(nil, positions, checkConstraintRow{}, proc))
}

func TestCollectCheckConstraintRowsFromResult(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	first := api.MustMarshalTblExtra(&api.SchemaExtra{
		Checks: []*planpb.CheckDef{
			{Name: "z_check", OriginSql: "z > 0"},
			{Name: "a_check", OriginSql: "a > 0"},
		},
	})
	second := api.MustMarshalTblExtra(&api.SchemaExtra{
		Checks: []*planpb.CheckDef{{Name: "c_check", OriginSql: "c > 0"}},
	})

	short := batch.NewWithSize(1)
	short.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(short.Vecs[0], []byte("ignored"), false, mp))
	short.SetRowCount(1)

	data := batch.NewWithSize(2)
	data.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	data.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	appendCatalogBytes := func(vec *vector.Vector, value []byte, isNull bool) {
		require.NoError(t, vector.AppendBytes(vec, value, isNull, mp))
	}
	appendCatalogBytes(data.Vecs[0], []byte("zdb"), false)
	appendCatalogBytes(data.Vecs[1], first, false)
	appendCatalogBytes(data.Vecs[0], []byte("adb"), false)
	appendCatalogBytes(data.Vecs[1], second, false)
	appendCatalogBytes(data.Vecs[0], nil, true)
	appendCatalogBytes(data.Vecs[1], first, false)
	appendCatalogBytes(data.Vecs[0], []byte("null_extra"), false)
	appendCatalogBytes(data.Vecs[1], nil, true)
	appendCatalogBytes(data.Vecs[0], []byte("empty_extra"), false)
	appendCatalogBytes(data.Vecs[1], []byte{}, false)
	data.SetRowCount(5)

	rows, err := collectCheckConstraintRowsFromResult(executor.Result{
		Mp:      mp,
		Batches: []*batch.Batch{short, data},
	})
	require.NoError(t, err)
	require.Equal(t, []checkConstraintRow{
		{schema: "adb", name: "c_check", clause: "c > 0"},
		{schema: "zdb", name: "a_check", clause: "a > 0"},
		{schema: "zdb", name: "z_check", clause: "z > 0"},
	}, rows)
}

func TestCollectCheckConstraintRowsFromResultRejectsMalformedMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewWithSize(2)
	data.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	data.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(data.Vecs[0], []byte("bad"), false, mp))
	require.NoError(t, vector.AppendBytes(data.Vecs[1], []byte{0xff}, false, mp))
	data.SetRowCount(1)

	rows, err := collectCheckConstraintRowsFromResult(executor.Result{
		Mp:      mp,
		Batches: []*batch.Batch{data},
	})
	require.Error(t, err)
	require.Nil(t, rows)
}

func TestCheckConstraintsPrepareAndStartSkipsNonZeroInputRows(t *testing.T) {
	proc := testutil.NewProc(t)
	tf := &TableFunction{
		FuncName: "mo_check_constraints",
		Attrs:    []string{},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	require.NoError(t, tf.Prepare(proc))
	state, ok := tf.ctr.state.(*checkConstraintsState)
	require.True(t, ok)
	require.NoError(t, state.start(tf, proc, 1, nil))
	require.NotNil(t, state.batch)
	tf.Free(proc, false, nil)
}

func TestCheckConstraintsStartUsesDecodedRowsAndAllColumns(t *testing.T) {
	proc := testutil.NewProc(t)
	tf := &TableFunction{
		FuncName: "mo_check_constraints",
		Attrs: []string{
			"constraint_catalog",
			"constraint_schema",
			"constraint_name",
			"check_clause",
		},
		Rets: []*planpb.ColDef{
			{Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Typ: planpb.Type{Id: int32(types.T_varchar)}},
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	require.NoError(t, tf.Prepare(proc))
	state := tf.ctr.state.(*checkConstraintsState)
	state.collectRows = func(*process.Process) ([]checkConstraintRow, error) {
		return []checkConstraintRow{{
			schema: "app",
			name:   "amount_positive",
			clause: "amount > 0",
		}}, nil
	}

	require.NoError(t, state.start(tf, proc, 0, nil))
	require.Equal(t, 1, state.batch.RowCount())
	require.Equal(t, "def", state.batch.Vecs[0].GetStringAt(0))
	require.Equal(t, "app", state.batch.Vecs[1].GetStringAt(0))
	require.Equal(t, "amount_positive", state.batch.Vecs[2].GetStringAt(0))
	require.Equal(t, "amount > 0", state.batch.Vecs[3].GetStringAt(0))
	tf.Free(proc, false, nil)
}
