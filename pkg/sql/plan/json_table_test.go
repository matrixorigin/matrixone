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
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
			_, err := runOneStmt(NewMockOptimizer(false), t,
				`SELECT * FROM JSON_TABLE('[]','$[*]' COLUMNS(`+tc.column+`)) jt`)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "invalid defaults must fail even for empty source")
			}
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
			p, err := runOneStmt(NewMockOptimizer(false), t, `SELECT n.n_name FROM nation n `+tc.join)
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
			_, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.ErrorContains(t, err, tc.errorText)
		})
	}
}
