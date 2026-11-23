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

// An inline LOAD's reader-visible ExternParam is reconstructed at execution
// from the scan node's serialized source param, exactly like a file-backed
// LOAD. Every parsed option the reader consumes (NullMap, FIELDS, LINES,
// IGNORE LINES, format, JSON type) must therefore survive planning in that
// serialization. The binary payload itself travels once, in ExternScan.Data;
// the serialized source param must not duplicate it.
func TestInlineLoadSerializesSourceParam(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		data string
	}{
		{
			name: "csv with set nullif and lines starting by",
			sql: "LOAD DATA INLINE FORMAT='csv', DATA='DATA:1,alpha,0\\nDATA:2,beta,7\\n' " +
				"INTO TABLE nation FIELDS TERMINATED BY ',' LINES STARTING BY 'DATA:' IGNORE 1 LINES " +
				"SET n_regionkey=NULLIF(n_regionkey,0)",
			data: "DATA:1,alpha,0\nDATA:2,beta,7\n",
		},
		{
			name: "jsonline with set nullif",
			sql: "LOAD DATA INLINE FORMAT='jsonline', DATA='{\"id\":1,\"flag\":0}\\n{\"id\":2,\"flag\":7}\\n', " +
				"JSONTYPE='object' INTO TABLE nation SET n_regionkey=NULLIF(n_regionkey,7)",
			data: "{\"id\":1,\"flag\":0}\n{\"id\":2,\"flag\":7}\n",
		},
		{
			name: "csv plain control without options",
			sql: "LOAD DATA INLINE FORMAT='csv', DATA='1,alpha,0\\n2,beta,7\\n' " +
				"INTO TABLE nation FIELDS TERMINATED BY ','",
			data: "1,alpha,0\n2,beta,7\n",
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
				require.Equal(t, int32(planpb.ExternType_LOAD), scan.ExternScan.Type)
				require.Equal(t, int32(tree.INLINE), scan.ExternScan.LoadType)
				require.Equal(t, tc.data, scan.ExternScan.Data, "payload must stay in ExternScan.Data")

				// The serialized source param carries the restored options.
				require.NotEmpty(t, scan.TableDef.Createsql,
					"the scan node must serialize the parsed source param for inline loads")
				got := &tree.ExternParam{}
				require.NoError(t, json.Unmarshal([]byte(scan.TableDef.Createsql), got))
				require.Equal(t, tree.INLINE, got.ScanType)
				require.Equal(t, "", got.Data, "serialized source param must not duplicate the payload")
				require.NotNil(t, got.Tail)

				switch tc.name {
				case "csv with set nullif and lines starting by":
					require.Equal(t, tree.CSV, got.Format)
					require.Empty(t, got.JsonData)
					require.Equal(t, map[string][]string{"n_regionkey": {"0"}}, got.NullMap)
					require.Equal(t, uint64(1), got.Tail.IgnoredLines)
					require.NotNil(t, got.Tail.Fields)
					require.Equal(t, ",", got.Tail.Fields.Terminated.Value)
					require.NotNil(t, got.Tail.Lines)
					require.Equal(t, "DATA:", got.Tail.Lines.StartingBy)
				case "jsonline with set nullif":
					require.Equal(t, tree.JSONLINE, got.Format)
					require.Equal(t, "object", got.JsonData)
					require.Equal(t, map[string][]string{"n_regionkey": {"7"}}, got.NullMap)
				case "csv plain control without options":
					require.Equal(t, tree.CSV, got.Format)
					require.Empty(t, got.NullMap)
					require.Nil(t, got.Tail.Lines)
				}
			})
		}
	}
}
