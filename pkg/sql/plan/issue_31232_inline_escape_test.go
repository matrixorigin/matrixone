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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Both planner surfaces encode the inline escape byte in the flat
// ExternScan.EscapedBy proto: byte 0 — escape processing disabled — when
// ESCAPED BY is omitted or empty, and the explicit byte otherwise. The
// serialized source param keeps the parser shape (nil for an omitted
// clause), and execution restores the escape byte from the proto, so the
// two encodings must stay in this relation for the reader to see the
// established inline escape behavior.
func TestInlineLoadEscapeBytePlanning(t *testing.T) {
	cases := []struct {
		name              string
		sql               string
		protoEscaped      byte
		serializedEscaped *tree.EscapedBy
	}{
		{
			name: "omitted ESCAPED BY",
			sql: "LOAD DATA INLINE FORMAT='csv', DATA='1,alpha,0\\n' " +
				"INTO TABLE nation FIELDS TERMINATED BY ','",
			protoEscaped:      0,
			serializedEscaped: nil,
		},
		{
			name: "explicit empty ESCAPED BY",
			sql: "LOAD DATA INLINE FORMAT='csv', DATA='1,alpha,0\\n' " +
				"INTO TABLE nation FIELDS TERMINATED BY ',' ESCAPED BY ''",
			protoEscaped:      0,
			serializedEscaped: &tree.EscapedBy{Value: 0},
		},
		{
			name: "explicit custom ESCAPED BY",
			sql: "LOAD DATA INLINE FORMAT='csv', DATA='1,alpha,0\\n' " +
				"INTO TABLE nation FIELDS TERMINATED BY ',' ESCAPED BY '|'",
			protoEscaped:      '|',
			serializedEscaped: &tree.EscapedBy{Value: '|'},
		},
	}

	planners := []struct {
		name  string
		build func(t *testing.T, sql string) *Plan
	}{
		{
			name: "binder",
			build: func(t *testing.T, sql string) *Plan {
				mock := NewMockOptimizer(true, newPlanTestProcess(t))
				pn, err := runOneStmt(mock, t, sql)
				require.NoError(t, err)
				return pn
			},
		},
		{
			name: "legacy",
			build: func(t *testing.T, sql string) *Plan {
				ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
				stmt, err := mysql.ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				pn, err := buildLoad(stmt.(*tree.Load), ctx, false)
				require.NoError(t, err)
				return pn
			},
		},
	}

	for _, planner := range planners {
		for _, tc := range cases {
			t.Run(planner.name+"/"+tc.name, func(t *testing.T) {
				pn := planner.build(t, tc.sql)
				var scan *planpb.Node
				for _, node := range pn.GetQuery().Nodes {
					if node.NodeType == planpb.Node_EXTERNAL_SCAN {
						require.Nil(t, scan, "expected exactly one EXTERNAL_SCAN node")
						scan = node
					}
				}
				require.NotNil(t, scan)
				require.Equal(t, []byte{tc.protoEscaped}, scan.ExternScan.EscapedBy,
					"the planners encode the inline escape byte in the flat proto")
				require.NotEmpty(t, scan.TableDef.Createsql)

				got := &tree.ExternParam{}
				require.NoError(t, json.Unmarshal([]byte(scan.TableDef.Createsql), got))
				require.NotNil(t, got.Tail)
				require.NotNil(t, got.Tail.Fields)
				require.Equal(t, tc.serializedEscaped, got.Tail.Fields.EscapedBy,
					"the serialized source param keeps the parser shape of the escape clause")
			})
		}
	}
}
