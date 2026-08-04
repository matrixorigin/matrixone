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
