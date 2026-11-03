// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/jsonvalue"
	"github.com/stretchr/testify/require"
)

func TestJSONTableDefaultAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, column string
		valid        bool
	}{
		{"integer", `n INT PATH '$' DEFAULT '7' ON EMPTY`, true},
		{"json_object", `n JSON PATH '$' DEFAULT '{"v":1}' ON EMPTY`, true},
		{"json_null", `n INT PATH '$' DEFAULT 'null' ON EMPTY`, true},
		{"successful_truncation", `n DECIMAL(10,2) PATH '$' DEFAULT '"12.345"' ON EMPTY`, true},
		{"invalid_json", `n INT PATH '$' DEFAULT 'invalid' ON EMPTY`, false},
		{"invalid_target_conversion", `n INT PATH '$' DEFAULT '"invalid"' ON EMPTY`, false},
		{"range", `n TINYINT PATH '$' DEFAULT '300' ON ERROR`, false},
		{"composite", `n INT PATH '$' DEFAULT '{}' ON ERROR`, false},
		{"unsupported_lossy_text", `n VARCHAR(1) PATH '$' DEFAULT '"ab"' ON EMPTY`, false},
		{"unsupported_fractional_integer", `n INT PATH '$' DEFAULT '1.25' ON EMPTY`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				`SELECT * FROM JSON_TABLE('[]','$[*]' COLUMNS(`+tc.column+`)) jt`)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "invalid defaults must fail even for empty source")
			}
		})
	}
}

func TestJSONTableColumnCharset(t *testing.T) {
	for _, tc := range []struct {
		name, column string
		wantType     types.T
		errCode      uint16
	}{
		{"omitted", `v VARCHAR(20) PATH '$'`, types.T_varchar, 0},
		{"varchar", `v VARCHAR(20) CHARACTER SET utf8mb4 PATH '$'`, types.T_varchar, 0},
		{"char", `v CHAR(4) CHAR SET UTF8MB4 PATH '$'`, types.T_char, 0},
		{"text", `v TEXT CHARSET 'utf8mb4' PATH '$'`, types.T_text, 0},
		{"utf8_alias", `v VARCHAR(20) CHARACTER SET utf8 PATH '$'`, types.T_varchar, 0},
		{"utf8mb3_alias", `v VARCHAR(20) CHARACTER SET utf8mb3 PATH '$'`, types.T_varchar, 0},
		{"nested", `NESTED PATH '$[*]' COLUMNS(v VARCHAR(20) CHARACTER SET utf8mb4 PATH '$')`, types.T_varchar, 0},
		{"exists", `v VARCHAR(20) CHARACTER SET utf8mb4 EXISTS PATH '$'`, types.T_varchar, 0},
		{"default", `v VARCHAR(20) CHARACTER SET utf8mb4 PATH '$' DEFAULT '"中😀"' ON EMPTY`, types.T_varchar, 0},
		{"empty_name", `v VARCHAR(20) CHARACTER SET '' PATH '$'`, 0, moerr.ErrNotSupported},
		{"unknown", `v VARCHAR(20) CHARACTER SET no_such_charset PATH '$'`, 0, moerr.ErrNotSupported},
		{"transcoding", `v VARCHAR(20) CHARACTER SET latin1 PATH '$'`, 0, moerr.ErrNotSupported},
		{"ddl_only_alias", `v VARCHAR(20) CHARACTER SET utf32 PATH '$'`, 0, moerr.ErrNotSupported},
		{"binary_conversion", `v CHAR(4) CHARACTER SET binary PATH '$'`, 0, moerr.ErrNotSupported},
		{"integer", `v INT CHARACTER SET utf8mb4 PATH '$'`, 0, moerr.ErrInvalidInput},
		{"binary_type", `v VARBINARY(20) CHARACTER SET utf8mb4 PATH '$'`, 0, moerr.ErrInvalidInput},
		{"json_type", `v JSON CHARACTER SET utf8mb4 PATH '$'`, 0, moerr.ErrInvalidInput},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Bind even an empty source: admission must not depend on emitted rows.
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				`SELECT * FROM JSON_TABLE('[]','$[*]' COLUMNS(`+tc.column+`)) jt`)
			if tc.errCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.errCode), "unexpected error: %v", err)
				return
			}
			require.NoError(t, err)
			var scans []*planpb.Node
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == planpb.Node_FUNCTION_SCAN && node.TableDef.GetTblFunc().GetName() == "json_table" {
					scans = append(scans, node)
				}
			}
			require.Len(t, scans, 1)
			require.Len(t, scans[0].TableDef.Cols, 1)
			typ := scans[0].TableDef.Cols[0].Typ
			require.Equal(t, int32(tc.wantType), typ.Id)
			require.Equal(t, uint32(types.CharsetUTF8), typ.Charset)
			require.Zero(t, typ.CollationVersion)

			var spec jsonvalue.TableSpec
			require.NoError(t, json.Unmarshal(scans[0].TableDef.TblFunc.Param, &spec))
			require.Equal(t, 1, spec.Version)
			require.Len(t, spec.Columns, 1)
			column := spec.Columns[0]
			if column.Kind == "nested" {
				require.Len(t, column.Children, 1)
				column = column.Children[0]
			}
			require.Equal(t, tc.wantType, column.Type.Oid)
			require.Equal(t, types.CharsetUTF8, column.Type.Charset)
			require.Zero(t, column.Type.CollationVersion)
		})
	}
}

func TestJSONTableCorrelatedJoinConditions(t *testing.T) {
	for _, tc := range []struct{ name, join string }{
		{"using", `JOIN JSON_TABLE(n.n_name,'$[*]' COLUMNS(n_nationkey INT PATH '$')) jt USING(n_nationkey)`},
		{"natural", `NATURAL JOIN JSON_TABLE(n.n_name,'$[*]' COLUMNS(n_nationkey INT PATH '$')) jt`},
		{"on", `JOIN JSON_TABLE(n.n_name,'$[*]' COLUMNS(n_nationkey INT PATH '$')) jt ON n.n_nationkey=jt.n_nationkey`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, `SELECT n.n_name FROM nation n `+tc.join)
			require.NoError(t, err)
			var applies, equalities int
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == planpb.Node_APPLY {
					applies++
					require.Equal(t, planpb.Node_CROSSAPPLY, node.ApplyType)
				}
				for _, filter := range node.FilterList {
					if f := filter.GetF(); f != nil && strings.EqualFold(f.Func.ObjName, "=") {
						equalities++
					}
				}
			}
			require.Positive(t, applies)
			require.Positive(t, equalities, "join predicate must not be dropped by APPLY lowering")
		})
	}
}

func TestJSONTableCoreFailClosed(t *testing.T) {
	for _, tc := range []struct{ sql, errorText string }{
		{`SELECT * FROM nation n LEFT JOIN JSON_TABLE(n.n_name,'$[*]' COLUMNS(v INT PATH '$')) jt ON true`, "requires INNER or CROSS JOIN"},
		{`SELECT * FROM JSON_TABLE('[]','$[*]' COLUMNS(v INT PATH '$' NULL ON ERROR NULL ON EMPTY)) jt`, "require parse-time diagnostics"},
		{`CREATE VIEW jt_core_view AS SELECT * FROM JSON_TABLE('[1]','$[*]' COLUMNS(v INT PATH '$')) jt`, "require the view compatibility gate"},
		{`SELECT * FROM JSON_TABLE(1,'$[*]' COLUMNS(v INT PATH '$')) jt`, "source must be JSON, text or DATALINK"},
	} {
		t.Run(tc.errorText, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.ErrorContains(t, err, tc.errorText)
		})
	}
}
