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

package plan

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	icebergmodel "github.com/matrixorigin/matrixone/pkg/iceberg/model"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// newExtIgnoreAdmissionMock registers one writable non-Iceberg external target
// (WRITE_FILE_PATTERN present), one read-only external target (no pattern), and
// one Iceberg mapping external target, each with columns id INT (nullable) and
// a INT NOT NULL. The mock catalog's built-in NATION table is the SELECT source.
func newExtIgnoreAdmissionMock(t *testing.T) *MockOptimizer {
	t.Helper()
	mock := NewMockOptimizer(true, newPlanTestProcess(t))

	newTarget := func(name string, tblId uint64, externSQL string) *planpb.TableDef {
		def := &planpb.TableDef{
			TblId:         tblId,
			Name:          name,
			TableType:     catalog.SystemExternalRel,
			Createsql:     externSQL,
			Name2ColIndex: map[string]int32{"id": 0, "a": 1},
			Cols: []*planpb.ColDef{
				{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32), Width: 32}, Default: &planpb.Default{NullAbility: true}},
				{Name: "a", Typ: planpb.Type{Id: int32(types.T_int32), Width: 32}, Default: &planpb.Default{NullAbility: false}},
			},
		}
		mock.ctxt.tables[name] = def
		mock.ctxt.objects[name] = &planpb.ObjectRef{Obj: int64(tblId), ObjName: name, SchemaName: mock.ctxt.DefaultDatabase()}
		return def
	}

	// Writable non-Iceberg external: the admission contract target.
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"filepath", "stage://ext_stage/p-*.csv", "format", "csv",
			"write_file_pattern", "stage://ext_stage/p-%U.csv"},
	}}
	metadata, err := json.Marshal(param)
	require.NoError(t, err)
	newTarget("ext_w_ignore", 9001, string(metadata))

	// Read-only external: modern InvalidInput rejection stays.
	roParam := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"filepath", "stage://ext_stage/p-ro-*.csv", "format", "csv"},
	}}
	roMetadata, err := json.Marshal(roParam)
	require.NoError(t, err)
	newTarget("ext_ro_ignore", 9002, string(roMetadata))

	// Iceberg mapping external: separate Iceberg DML rejection stays.
	iceSQL := sqliceberg.BuildCreateSQLEnvelope(icebergmodel.TableMapping{
		Namespace:  "ext_ignore",
		TableName:  "ice",
		DefaultRef: icebergmodel.DefaultRefMain,
		ReadMode:   icebergmodel.ReadModeMergeOnRead,
		WriteMode:  icebergmodel.WriteModeMergeOnRead,
	}, "ksa_ext_ignore")
	newTarget("ice_ext_ignore", 9003, iceSQL)

	return mock
}

// requireExternalInsertPlan asserts the plan bound through the legacy writable
// external insert path: an INSERT query whose sink node carries the external
// table with an INSERT statement.
func requireExternalInsertPlan(t *testing.T, p *Plan, targetTable string) {
	t.Helper()
	q := p.GetQuery()
	require.NotNil(t, q)
	require.Equal(t, planpb.Query_INSERT, q.GetStmtType())
	var insertNode *planpb.Node
	for _, node := range q.GetNodes() {
		if node.GetNodeType() == planpb.Node_INSERT {
			insertNode = node
			break
		}
	}
	require.NotNil(t, insertNode, "expected an INSERT sink node")
	require.NotNil(t, insertNode.GetInsertCtx(), "external insert sink must carry an INSERT context")
	require.True(t, insertNode.GetInsertCtx().GetAddAffectedRows())
	require.Equal(t, catalog.SystemExternalRel, insertNode.GetTableDef().GetTableType())
	require.Equal(t, targetTable, insertNode.GetTableDef().GetName())
}

// TestExternalInsertIgnoreAdmission covers issue #32512: INSERT IGNORE into a
// writable non-Iceberg external table must bind through the existing legacy
// external insert path (VALUES / SELECT / prepared forms), while every
// rejection classification stays unchanged: read-only external (20301),
// external ODKU / Iceberg (20313), RETURNING (not supported), and non-Iceberg
// OVERWRITE (not supported).
func TestExternalInsertIgnoreAdmission(t *testing.T) {
	positives := []struct {
		name     string
		sql      string
		prepared bool
	}{
		{"values", "INSERT IGNORE INTO ext_w_ignore VALUES (1, 1)", false},
		{"values_null_literal", "INSERT IGNORE INTO ext_w_ignore VALUES (2, NULL)", false},
		{"values_column_list", "INSERT IGNORE INTO ext_w_ignore (id, a) VALUES (3, 3)", false},
		{"select_source", "INSERT IGNORE INTO ext_w_ignore SELECT n.N_NATIONKEY, 0 FROM NATION n", false},
		{"prepared_values", "INSERT IGNORE INTO ext_w_ignore VALUES (1, 1)", true},
		{"prepared_select", "INSERT IGNORE INTO ext_w_ignore SELECT n.N_NATIONKEY, 0 FROM NATION n", true},
	}
	for _, tc := range positives {
		t.Run("admit/"+tc.name, func(t *testing.T) {
			mock := newExtIgnoreAdmissionMock(t)
			stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), tc.sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			p, err := BuildPlan(mock.CurrentContext(), stmts[0], tc.prepared, nil)
			require.NoError(t, err)
			requireExternalInsertPlan(t, p, "ext_w_ignore")
		})
	}

	controls := []struct {
		name     string
		sql      string
		prepared bool
		wantCode uint16
		wantMsg  string
	}{
		{"readonly_external_ignore", "INSERT IGNORE INTO ext_ro_ignore VALUES (1, 1)", false,
			moerr.ErrInvalidInput, "cannot insert/update/delete from external table"},
		{"writable_external_ignore_odku", "INSERT IGNORE INTO ext_w_ignore VALUES (1, 1) ON DUPLICATE KEY UPDATE a = 2", false,
			moerr.ErrUnsupportedDML, "unsupported DML: external table"},
		{"writable_external_plain_odku", "INSERT INTO ext_w_ignore VALUES (1, 1) ON DUPLICATE KEY UPDATE a = 2", false,
			moerr.ErrUnsupportedDML, "unsupported DML: external table"},
		{"iceberg_ignore", "INSERT IGNORE INTO ice_ext_ignore VALUES (1, 1)", false,
			moerr.ErrUnsupportedDML, "unsupported DML: Iceberg row-level DML"},
		{"iceberg_ignore_prepared", "INSERT IGNORE INTO ice_ext_ignore VALUES (1, 1)", true,
			moerr.ErrUnsupportedDML, "unsupported DML: Iceberg row-level DML"},
		{"returning_external", "INSERT INTO ext_w_ignore VALUES (1, 1) RETURNING id", false,
			moerr.ErrNotSupported, "DML RETURNING does not support external table"},
		{"returning_external_ignore", "INSERT IGNORE INTO ext_w_ignore VALUES (1, 1) RETURNING id", false,
			moerr.ErrNotSupported, "DML RETURNING does not support INSERT IGNORE"},
		{"plain_overwrite_external", "INSERT OVERWRITE INTO ext_w_ignore VALUES (1, 1)", false,
			moerr.ErrNotSupported, "INSERT OVERWRITE on non-Iceberg external table"},
	}
	for _, tc := range controls {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			mock := newExtIgnoreAdmissionMock(t)
			stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), tc.sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			_, err = BuildPlan(mock.CurrentContext(), stmts[0], tc.prepared, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, tc.wantCode), "got %q, want code %v", err.Error(), tc.wantCode)
			require.Contains(t, err.Error(), tc.wantMsg)
		})
	}

	// Ordinary plain INSERT into the writable external target keeps working
	// through the unchanged legacy fallback both before and after the fix.
	t.Run("plain_insert_external_unchanged", func(t *testing.T) {
		mock := newExtIgnoreAdmissionMock(t)
		stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), "INSERT INTO ext_w_ignore VALUES (1, 1)", 1)
		require.NoError(t, err)
		defer stmts[0].Free()
		p, err := BuildPlan(mock.CurrentContext(), stmts[0], false, nil)
		require.NoError(t, err)
		requireExternalInsertPlan(t, p, "ext_w_ignore")
	})
}
