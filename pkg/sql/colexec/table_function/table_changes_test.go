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

package table_function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestParseTableChangesTS(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		allowEmpty bool
		want       types.TS
		wantErr    bool
	}{
		{name: "empty lower bound", value: "", allowEmpty: true},
		{name: "valid", value: "123-7", want: types.BuildTS(123, 7)},
		{name: "empty upper bound", value: "", wantErr: true},
		{name: "missing logical", value: "123", wantErr: true},
		{name: "negative physical", value: "-1-0", wantErr: true},
		{name: "logical overflow", value: "1-4294967296", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTableChangesTS(tt.value, tt.allowEmpty)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestValidateTableChangesWindow(t *testing.T) {
	snapshot := types.BuildTS(100, 5)

	require.NoError(t, validateTableChangesWindow(types.BuildTS(99, 0), snapshot, snapshot))
	require.NoError(t, validateTableChangesWindow(types.TS{}, snapshot, snapshot))
	require.EqualError(t,
		validateTableChangesWindow(types.BuildTS(99, 0), snapshot.Next(), snapshot),
		"invalid input: table_changes until must not be newer than the statement snapshot",
	)
	require.EqualError(t,
		validateTableChangesWindow(snapshot, snapshot, snapshot),
		"invalid input: table_changes until must be greater than after",
	)
}

func TestValidateRuntimeTableChangesSourceTemporaryTable(t *testing.T) {
	err := validateRuntimeTableChangesSource(&plan.TableDef{
		TableType:   catalog.SystemTemporaryTable,
		IsTemporary: true,
	})
	require.EqualError(t, err, "not supported: table_changes does not support temporary tables")
}

func TestValidateRuntimeTableChangesSourceContracts(t *testing.T) {
	valid := func() *plan.TableDef {
		return &plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Pkey:      &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		}
	}
	require.EqualError(t, validateRuntimeTableChangesSource(nil),
		"invalid input: table_changes source table does not exist")

	unsupported := valid()
	unsupported.TableType = catalog.SystemViewRel
	require.ErrorContains(t, validateRuntimeTableChangesSource(unsupported),
		"table_changes does not support table type")

	partitioned := valid()
	partitioned.Partition = &plan.Partition{}
	require.EqualError(t, validateRuntimeTableChangesSource(partitioned),
		"not supported: table_changes does not support partitioned tables")

	for _, pkey := range []*plan.PrimaryKeyDef{
		nil,
		{},
		{Names: []string{"id"}, PkeyColName: catalog.FakePrimaryKeyColName},
	} {
		withoutPK := valid()
		withoutPK.Pkey = pkey
		require.EqualError(t, validateRuntimeTableChangesSource(withoutPK),
			"not supported: table_changes requires an explicit primary key")
	}

	cluster := valid()
	cluster.TableType = catalog.SystemClusterRel
	require.EqualError(t, validateRuntimeTableChangesSource(cluster),
		"not supported: table_changes requires cluster table primary keys to include account_id")
	cluster.Pkey.Names = append(cluster.Pkey.Names, "ACCOUNT_ID")
	require.NoError(t, validateRuntimeTableChangesSource(cluster))
	require.True(t, containsTableChangesKey(cluster.Pkey.Names, "account_id"))
	require.False(t, containsTableChangesKey(cluster.Pkey.Names, "missing"))
}

func TestRequiredTableChangesString(t *testing.T) {
	proc := testutil.NewProc(t)
	stringsVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(stringsVec, []byte("value"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(stringsVec, nil, true, proc.Mp()))
	defer stringsVec.Free(proc.Mp())

	value, err := requiredTableChangesString(proc, stringsVec, 0, "argument")
	require.NoError(t, err)
	require.Equal(t, "value", value)
	_, err = requiredTableChangesString(proc, stringsVec, 1, "argument")
	require.ErrorContains(t, err, "cannot be NULL")
	_, err = requiredTableChangesString(proc, nil, 0, "argument")
	require.ErrorContains(t, err, "cannot be NULL")

	intVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(intVec, int64(1), false, proc.Mp()))
	defer intVec.Free(proc.Mp())
	_, err = requiredTableChangesString(proc, intVec, 0, "argument")
	require.ErrorContains(t, err, "must be a string")
}

func TestTableChangesAppendInsertAndDeleteRows(t *testing.T) {
	proc := testutil.NewProc(t)
	attrs := []string{
		catalog.TableChangesAttrChangeType,
		catalog.TableChangesAttrCommitTS,
		catalog.TableChangesAttrTableID,
		catalog.TableChangesAttrSchemaVersion,
		"id",
		"payload",
	}
	state := &tableChangesState{
		tableDef: &plan.TableDef{
			TblId: 42, Version: 7,
			Name2ColIndex: map[string]int32{"id": 0, "payload": 1},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		},
		batch: batch.NewWithSize(len(attrs)),
	}
	for idx, typ := range []types.Type{
		types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_uint64.ToType(), types.T_uint32.ToType(),
		types.T_int64.ToType(), types.T_varchar.ToType(),
	} {
		state.batch.Vecs[idx] = vector.NewVec(typ)
	}
	defer state.batch.Clean(proc.Mp())

	commitTS := types.BuildTS(10, 2)
	inserts := batch.NewWithSize(3)
	inserts.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	inserts.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	inserts.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(inserts.Vecs[0], int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(inserts.Vecs[1], []byte("inserted"), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(inserts.Vecs[2], commitTS, false, proc.Mp()))
	inserts.SetRowCount(1)
	require.NoError(t, state.appendInsertRows(attrs, inserts, proc))
	inserts.Clean(proc.Mp())

	deletes := batch.NewWithSize(2)
	deletes.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	deletes.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(deletes.Vecs[0], int64(2), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(deletes.Vecs[1], commitTS.Next(), false, proc.Mp()))
	deletes.SetRowCount(1)
	require.NoError(t, state.appendDeleteRows(attrs, deletes, proc))
	deletes.Clean(proc.Mp())

	require.Equal(t, 2, state.batch.RowCount())
	require.Equal(t, []string{"insert", "delete"}, vector.InefficientMustStrCol(state.batch.Vecs[0]))
	require.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](state.batch.Vecs[4]))
	require.Equal(t, "inserted", state.batch.Vecs[5].GetStringAt(0))
	require.True(t, state.batch.Vecs[5].IsNull(1))
	require.NoError(t, state.appendInsertRows(attrs, nil, proc))
	require.NoError(t, state.appendDeleteRows(attrs, nil, proc))
}

func TestTableChangesDeleteKeyNamesAreCaseInsensitive(t *testing.T) {
	proc := testutil.NewProc(t)
	attrs := []string{"MixedCasePK"}
	state := &tableChangesState{
		tableDef: &plan.TableDef{
			Name2ColIndex: map[string]int32{"mixedcasepk": 0},
			Pkey:          &plan.PrimaryKeyDef{Names: attrs, PkeyColName: attrs[0]},
		},
		batch: batch.NewWithSize(1),
	}
	state.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer state.batch.Clean(proc.Mp())

	deletes := batch.NewWithSize(2)
	deletes.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	deletes.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(deletes.Vecs[0], int64(7), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(deletes.Vecs[1], types.BuildTS(10, 2), false, proc.Mp()))
	deletes.SetRowCount(1)
	defer deletes.Clean(proc.Mp())

	require.NoError(t, state.appendDeleteRows(attrs, deletes, proc))
	require.Equal(t, []int64{7}, vector.MustFixedColWithTypeCheck[int64](state.batch.Vecs[0]))
}

func TestValidateRuntimeTableChangesSourceRejectsMetadataColumnNames(t *testing.T) {
	for _, name := range []string{
		catalog.TableChangesAttrChangeType,
		catalog.TableChangesAttrCommitTS,
		catalog.TableChangesAttrTableID,
		catalog.TableChangesAttrSchemaVersion,
	} {
		t.Run(name, func(t *testing.T) {
			err := validateRuntimeTableChangesColumnNames(&plan.TableDef{
				Cols: []*plan.ColDef{{Name: name}},
			})
			require.EqualError(t, err,
				"invalid input: table_changes source column \""+name+"\" conflicts with reserved metadata column")
		})
	}
}

func TestValidateTableChangesSchemaIdentity(t *testing.T) {
	current := &plan.TableDef{TblId: 7, Version: 2}

	require.NoError(t, validateTableChangesSchemaIdentity(
		current,
		&plan.TableDef{TblId: 7, Version: 2},
		&plan.TableDef{TblId: 7, Version: 2},
		false,
	))
	for _, tc := range []struct {
		name  string
		after *plan.TableDef
		until *plan.TableDef
	}{
		{name: "add or drop changes version at after", after: &plan.TableDef{TblId: 7, Version: 1}, until: current},
		{name: "type change changes version at until", after: current, until: &plan.TableDef{TblId: 7, Version: 3}},
		{name: "drop and recreate changes table identity", after: &plan.TableDef{TblId: 6, Version: 2}, until: current},
		{name: "table absent at range endpoint", after: current, until: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTableChangesSchemaIdentity(current, tc.after, tc.until, false)
			require.EqualError(t, err,
				"not supported: table_changes requires a single source schema version across after, until, and the query snapshot")
		})
	}

	require.NoError(t, validateTableChangesSchemaIdentity(
		&plan.TableDef{TblId: 7, Version: 0}, nil,
		&plan.TableDef{TblId: 7, Version: 0}, true,
	))
	require.Error(t, validateTableChangesSchemaIdentity(current, nil, current, true))
}

func TestTableChangesCleansBatchesReturnedWithError(t *testing.T) {
	proc := testutil.NewProc(t)
	state := &tableChangesState{handle: &errorWithBatchChangesHandle{}}

	_, err := state.call(&TableFunction{}, proc)
	require.EqualError(t, err, "internal error: allocated read failure")
	require.Zero(t, proc.Mp().CurrNB())
}

type errorWithBatchChangesHandle struct{}

func (h *errorWithBatchChangesHandle) Next(
	_ context.Context,
	mp *mpool.MPool,
) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	if err := vector.AppendFixed(data.Vecs[0], int64(1), false, mp); err != nil {
		return data, nil, engine.ChangesHandle_Tail_done, err
	}
	data.SetRowCount(1)
	return data, nil, engine.ChangesHandle_Tail_done,
		moerr.NewInternalErrorNoCtx("allocated read failure")
}

func (h *errorWithBatchChangesHandle) Close() error { return nil }

func TestTableChangesReadFailpoints(t *testing.T) {
	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })

	for _, point := range []string{"collect", "next"} {
		t.Run(point, func(t *testing.T) {
			remove, err := objectio.InjectTableChangesRead(point)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = remove()
			})

			require.NoError(t, tableChangesReadFailpoint("other"))
			require.EqualError(t, tableChangesReadFailpoint(point),
				"internal error: table_changes injected "+point+" failure")
			_, err = remove()
			require.NoError(t, err)
			require.NoError(t, tableChangesReadFailpoint(point))
		})
	}
}
