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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
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
