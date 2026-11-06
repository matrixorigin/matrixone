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
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// A prepared LOAD statement replays planning from the retained AST when a
// session mode such as ONLY_FULL_GROUP_BY changes between PREPARE and
// EXECUTE: freshPreparedCloneStatement returns the retained statement
// unchanged and rebuildPreparePlan plans it again. InitNullMap keeps the
// normalized rules only while the consumed assignments remain attached to
// that AST, so the serialization used by both planner surfaces must not
// consume them. Every one of those planning passes must serialize the same
// SET NULLIF mapping.
func TestSerializedSourceParamSurvivesReplan(t *testing.T) {
	const sql = "LOAD DATA INLINE FORMAT='csv', DATA='1,alpha,0\\n2,beta,7\\n' " +
		"INTO TABLE nation FIELDS TERMINATED BY ',' SET n_regionkey=NULLIF(n_regionkey,0)"
	wantNullMap := map[string][]string{"n_regionkey": {"0"}}

	extractScan := func(t *testing.T, pn *Plan) *planpb.Node {
		var scan *planpb.Node
		for _, node := range pn.GetQuery().Nodes {
			if node.NodeType == planpb.Node_EXTERNAL_SCAN {
				require.Nil(t, scan, "expected exactly one EXTERNAL_SCAN node")
				scan = node
			}
		}
		require.NotNil(t, scan)
		return scan
	}
	deserialize := func(t *testing.T, scan *planpb.Node) *tree.ExternParam {
		require.NotEmpty(t, scan.TableDef.Createsql)
		got := &tree.ExternParam{}
		require.NoError(t, json.Unmarshal([]byte(scan.TableDef.Createsql), got))
		return got
	}

	t.Run("full legacy double plan", func(t *testing.T) {
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		stmt, err := mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		load, ok := stmt.(*tree.Load)
		require.True(t, ok)

		pn, err := buildLoad(load, ctx, false)
		require.NoError(t, err)
		require.Equal(t, wantNullMap, deserialize(t, extractScan(t, pn)).NullMap)

		// The retained AST is planned a second time: a mode-driven prepared
		// rebuild does exactly this, on the same statement object.
		pn2, err := buildLoad(load, ctx, false)
		require.NoError(t, err)
		got := deserialize(t, extractScan(t, pn2))
		require.Equal(t, wantNullMap, got.NullMap,
			"a rebuild from the retained AST must keep the normalized NULLIF rules")
		require.Empty(t, got.Data, "the transport must not duplicate the payload")
		require.Nil(t, got.Tail.Assignments, "the transport must not carry binding-consumed assignment trees")
		require.NotEmpty(t, load.Param.Tail.Assignments,
			"the caller's AST must keep its consumed assignments as the replan marker")
	})

	t.Run("binder replay sequence", func(t *testing.T) {
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		stmt, err := mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		param := stmt.(*tree.Load).Param

		// First planning pass: normalize, then serialize.
		require.NoError(t, InitNullMap(param, ctx))
		firstPass, err := marshalLoadSourceParam(param)
		require.NoError(t, err)
		got1 := &tree.ExternParam{}
		require.NoError(t, json.Unmarshal([]byte(firstPass), got1))
		require.Equal(t, wantNullMap, got1.NullMap)

		// Replay pass on the retained AST: normalize, then serialize again.
		require.NoError(t, InitNullMap(param, ctx))
		replay, err := marshalLoadSourceParam(param)
		require.NoError(t, err)
		got2 := &tree.ExternParam{}
		require.NoError(t, json.Unmarshal([]byte(replay), got2))
		require.Equal(t, wantNullMap, got2.NullMap,
			"a replay from the retained AST must keep the normalized NULLIF rules")
		require.Nil(t, got2.Tail.Assignments)
		require.NotEmpty(t, param.Tail.Assignments,
			"the caller's AST must keep its consumed assignments as the replan marker")
	})
}

// A LOAD that routes a raw field through a row variable and a SET assignment
// re-derives the scan-column and NULLIF projection from the retained
// statement: both planner surfaces call loadUserVariableNullIfAssignments
// before InitNullMap and rebind a copy of the row-variable wrapper on every
// pass (bind_load.go and build_load.go share the same helpers). InitNullMap
// consumes column-NULLIF expressions but must keep those row-variable
// wrappers attached, so a replan of the same retained AST (a PREPARE rebuilt
// after a session-mode change) still assigns the supplied nonmatching value
// and maps the matching value to NULL instead of falling back to the target
// column default.
func TestSerializedSourceParamSurvivesUserVariableReplan(t *testing.T) {
	const sql = "LOAD DATA INLINE FORMAT='csv', DATA='1,keep\\n2,sentinel\\n' " +
		"INTO TABLE nation FIELDS TERMINATED BY ',' (n_nationkey, @v) SET n_name=NULLIF(@v,'sentinel')"

	t.Run("full legacy double plan rebuilds the row-variable projection", func(t *testing.T) {
		// Every real column is listed (the committed issue-29849 builder tests
		// use the same shape), so both passes supply each non-target column
		// from the data and only the assignment target n_comment depends on
		// the rebindable wrapper. If a replan lost that wrapper the target
		// would fall back to its default (NULL), so the replanned plan must
		// differ in the scan column set and in the assignment projection.
		const legacySQL = "LOAD DATA INLINE FORMAT='csv', DATA='1,keep\\n2,sentinel\\n' " +
			"INTO TABLE nation FIELDS TERMINATED BY ',' (n_nationkey, n_regionkey, n_name, @v) " +
			"SET n_comment=NULLIF(@v,'sentinel')"
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		stmt, err := mysql.ParseOne(t.Context(), legacySQL, 1)
		require.NoError(t, err)
		defer stmt.Free()
		load, ok := stmt.(*tree.Load)
		require.True(t, ok)

		// The legacy LOAD DATA builder is normally entered after DML
		// resolution has populated this index (resolveSingleTable, and the
		// engine's GetTableDef, build it). Seed it in this direct builder
		// test to match that precondition, as the issue-29849 builder tests
		// do. Both passes resolve a deep copy, so the seeded map serves the
		// replan too.
		nation := ctx.tables["nation"]
		require.NotNil(t, nation)
		nation.Name2ColIndex = make(map[string]int32, len(nation.Cols))
		for i, column := range nation.Cols {
			nation.Name2ColIndex[column.Name] = int32(i)
		}

		first, err := buildLoad(load, ctx, false)
		require.NoError(t, err)
		second, err := buildLoad(load, ctx, false) // replan of the retained AST
		require.NoError(t, err)

		for name, pn := range map[string]*Plan{"first plan": first, "replanned plan": second} {
			var scanIndex int
			var scan *planpb.Node
			for i, node := range pn.GetQuery().Nodes {
				if node.NodeType == planpb.Node_EXTERNAL_SCAN {
					require.Nil(t, scan, "expected exactly one EXTERNAL_SCAN node")
					scan = node
					scanIndex = i
				}
			}
			require.NotNil(t, scan)
			var userVar, skipVar bool
			for _, col := range scan.TableDef.Cols {
				if strings.Contains(col.Name, "__mo_load_user_var_") {
					userVar = true
				}
				if strings.Contains(col.Name, "__mo_load_skip_var_") {
					skipVar = true
				}
			}
			require.True(t, userVar,
				name+": the referenced row variable must stay a projected scan column")
			require.False(t, skipVar,
				name+": a referenced row variable must not be recorded as a skipped slot")

			var project *planpb.Node
			for _, node := range pn.GetQuery().Nodes {
				if node.NodeType == planpb.Node_PROJECT && len(node.Children) == 1 && node.Children[0] == int32(scanIndex) {
					require.Nil(t, project, "expected exactly one LOAD projection over the scan")
					project = node
				}
			}
			require.NotNil(t, project, name+": the LOAD projection over the scan must exist")
			require.Len(t, project.ProjectList, 4)
			var assignment *planpb.Function
			for _, entry := range project.ProjectList {
				if fn := entry.GetF(); fn != nil {
					require.Nil(t, assignment,
						name+": only the row-variable assignment may be a function expression")
					assignment = fn
				}
			}
			require.NotNil(t, assignment,
				name+": the assignment target must be supplied by the row-variable projection, not the column default")
			require.Equal(t, "cast_assign", assignment.GetFunc().GetObjName(),
				name+": the row-variable assignment must keep its assignment cast")

			got := &tree.ExternParam{}
			require.NoError(t, json.Unmarshal([]byte(scan.TableDef.Createsql), got))
			require.Equal(t, "", got.Data, name+": transport must not duplicate the payload")
			require.Nil(t, got.Tail.Assignments, name+": transport stays clean")
		}

		require.Len(t, load.Param.Tail.ColumnList, 4,
			"the retained AST must keep the column list for the next plan")
		require.NotEmpty(t, load.Param.Tail.Assignments,
			"the retained AST must keep the assignment entries")
	})

	cases := []struct {
		name         string
		sql          string
		transportNMs map[string][]string
	}{
		{
			name:         "row variable only",
			sql:          sql,
			transportNMs: map[string][]string{},
		},
		{
			name: "mixed with column target first",
			sql: "LOAD DATA INLINE FORMAT='csv', DATA='1,keep\\n2,sentinel\\n' " +
				"INTO TABLE nation FIELDS TERMINATED BY ',' (n_nationkey, n_regionkey, @v) " +
				"SET n_regionkey=NULLIF(n_regionkey,0), n_name=NULLIF(@v,'sentinel')",
			transportNMs: map[string][]string{"n_regionkey": {"0"}},
		},
		{
			name: "mixed with row variable first",
			sql: "LOAD DATA INLINE FORMAT='csv', DATA='1,keep\\n2,sentinel\\n' " +
				"INTO TABLE nation FIELDS TERMINATED BY ',' (n_nationkey, n_regionkey, @v) " +
				"SET n_name=NULLIF(@v,'sentinel'), n_regionkey=NULLIF(n_regionkey,0)",
			transportNMs: map[string][]string{"n_regionkey": {"0"}},
		},
	}

	t.Run("binder replay sequence keeps the assignment rebindable", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
				stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				param := stmt.(*tree.Load).Param

				// First pass in the shared planner order.
				require.NoError(t, validateLoadSetAssignments(param, ctx, ctx.tables["nation"]))
				assignments, err := loadUserVariableNullIfAssignments(param, ctx)
				require.NoError(t, err)
				require.Len(t, assignments, 1, "first pass must find the row-variable assignment")
				require.Equal(t, "n_name", assignments[0].target)
				require.Equal(t, "v", assignments[0].variable)
				require.NoError(t, InitNullMap(param, ctx))
				_, err = marshalLoadSourceParam(param)
				require.NoError(t, err)

				// Replay: the projection-rebinding information must survive.
				require.NoError(t, validateLoadSetAssignments(param, ctx, ctx.tables["nation"]))
				replay, err := loadUserVariableNullIfAssignments(param, ctx)
				require.NoError(t, err)
				require.Len(t, replay, 1,
					"a replan must still find the row-variable assignment to rebuild the projection")
				require.Equal(t, "n_name", replay[0].target)
				require.Equal(t, "v", replay[0].variable)
				require.NoError(t, InitNullMap(param, ctx))
				serialized, err := marshalLoadSourceParam(param)
				require.NoError(t, err)
				got := &tree.ExternParam{}
				require.NoError(t, json.Unmarshal([]byte(serialized), got))
				require.Equal(t, tree.INLINE, got.ScanType)
				require.Equal(t, "", got.Data)
				require.Equal(t, tc.transportNMs, got.NullMap,
					"every planning pass must serialize the same column NULLIF rules")
			})
		}
	})
}
