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

package colexec_test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestProjectionJSONPlannedJoinedUpdate(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	ctx := &orLikeStringBindingContext{MockCompilerContext: plan2.NewMockCompilerContext(true, proc), columnType: types.T_text}
	stmt, err := mysql.ParseOne(proc.Ctx, `update tpch.orders t join tpch.lineitem m on t.o_orderkey=m.l_orderkey
		set t.o_comment=coalesce(json_unquote(json_extract(m.l_comment,'$.a')),''),
		    t.o_clerk=coalesce(json_unquote(json_extract(m.l_comment,'$.b')),'')`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := plan2.BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)

	var targets []*plan.Expr
	var docs []*plan.Expr
	var visit func(expr *plan.Expr, found *[]*plan.Expr)
	visit = func(expr *plan.Expr, found *[]*plan.Expr) {
		if f := expr.GetF(); f != nil {
			id, _ := function.DecodeOverloadID(f.GetFunc().GetObj())
			if id == function.JSON_EXTRACT {
				*found = append(*found, f.Args[0])
			}
			for _, arg := range f.Args {
				visit(arg, found)
			}
		}
	}
	for _, node := range built.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			var found []*plan.Expr
			visit(expr, &found)
			if len(found) != 0 {
				targets = append(targets, expr)
				docs = append(docs, found...)
			}
		}
	}
	require.Len(t, targets, 2)
	require.Len(t, docs, 2)
	require.NotNil(t, docs[0].GetCol())
	require.Equal(t, docs[0].GetCol(), docs[1].GetCol())
	require.Equal(t, int32(types.T_text), docs[0].Typ.Id)
	execs, frame, err := colexec.NewProjectionExecutors(proc, targets, nil)
	require.NoError(t, err)
	for _, exec := range execs {
		defer exec.Free()
	}
	require.NotNil(t, frame, "the assignment projection from the real UPDATE plan must share document parsing")
	parses := colexec.CountProjectionJSONParses(t, frame)

	var visitColumns func(*plan.Expr)
	columns := make(map[int32]plan.Type)
	visitColumns = func(expr *plan.Expr) {
		if col := expr.GetCol(); col != nil {
			require.Zero(t, col.RelPos)
			require.GreaterOrEqual(t, col.ColPos, int32(0))
			columns[col.ColPos] = expr.Typ
		}
		if f := expr.GetF(); f != nil {
			for _, arg := range f.Args {
				visitColumns(arg)
			}
		}
	}
	maxCol := int32(0)
	for _, expr := range targets {
		visitColumns(expr)
	}
	for pos := range columns {
		maxCol = max(maxCol, pos)
	}
	input := batch.NewWithSize(int(maxCol) + 1)
	defer input.Clean(proc.Mp())
	input.SetRowCount(3)
	for pos, typ := range columns {
		vec := vector.NewVec(types.MustTypeFromPlan(typ))
		input.Vecs[pos] = vec
		switch {
		case pos == docs[0].GetCol().ColPos:
			require.NoError(t, vector.AppendStringList(vec, []string{`{"a":"one","b":"two"}`, "malformed", "{}"}, nil, proc.Mp()))
		case types.T(typ.Id) == types.T_Rowid:
			require.NoError(t, vector.AppendFixedList(vec, make([]types.Rowid, 3), []bool{false, true, false}, proc.Mp()))
		case types.T(typ.Id) == types.T_int64:
			require.NoError(t, vector.AppendFixedList(vec, []int64{1, 2, 1}, nil, proc.Mp()))
		case types.T(typ.Id).IsMySQLString():
			require.NoError(t, vector.AppendStringList(vec, []string{"old", "old", "old"}, nil, proc.Mp()))
		default:
			t.Fatalf("unexpected joined UPDATE column type %v", typ)
		}
	}

	frame.Begin(input)
	defer frame.End()
	firstRow := make([]string, 0, len(execs))
	for _, exec := range execs {
		got, err := exec.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		firstRow = append(firstRow, got.UnsafeGetStringAt(0))
		require.Equal(t, "old", got.UnsafeGetStringAt(1), "a non-target row keeps its previous value without parsing")
		require.Empty(t, got.UnsafeGetStringAt(2), "missing paths take the COALESCE default")
	}
	require.ElementsMatch(t, []string{"one", "two"}, firstRow)
	require.Equal(t, 2, *parses, "both assignments must reuse each selected document's parse")
}
