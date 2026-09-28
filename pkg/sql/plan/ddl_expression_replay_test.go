// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPersistedDDLReplayRemapsExistingExpressions(t *testing.T) {
	decimal := planpb.Type{Id: 10, Width: 10, Scale: 2}
	ref := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: decimal, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
	}
	division := func(a, b int32) *planpb.Expr {
		return &planpb.Expr{Typ: decimal, Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{ref(a), ref(b)}}}}
	}
	old := &planpb.TableDef{Name: "copy", Cols: []*planpb.ColDef{
		{ColId: 1, Name: "a", Typ: decimal},
		{ColId: 2, Name: "b", Typ: decimal},
		{ColId: 3, Name: "q", Typ: decimal, GeneratedCol: &planpb.GeneratedCol{Expr: division(0, 1)}},
	}, Checks: []*planpb.CheckDef{{Name: "valid", Check: division(0, 1)}}}
	target := &planpb.TableDef{Name: "copy", Cols: []*planpb.ColDef{
		{ColId: 4, Name: "note", Typ: planpb.Type{Id: 6}},
		{ColId: 1, Name: "a", Typ: decimal},
		{ColId: 2, Name: "b", Typ: decimal},
		{ColId: 3, Name: "q", Typ: decimal, GeneratedCol: &planpb.GeneratedCol{Expr: division(1, 2)}},
	}, Checks: []*planpb.CheckDef{{Name: "valid", Check: division(0, 1)}}}

	replay := ddlReplayForTable(WithPersistedDDLReplay(context.Background(), old, target), "copy")
	require.NotNil(t, replay)
	require.Equal(t, int32(1), replay.columns["q"].generated.Expr.GetF().Args[0].GetCol().ColPos)
	require.Equal(t, int32(2), replay.checks["valid"].Check.GetF().Args[1].GetCol().ColPos)
	// Replay construction must not rewrite the catalog source or target plan.
	require.Equal(t, int32(0), old.Cols[2].GeneratedCol.Expr.GetF().Args[0].GetCol().ColPos)
	require.Equal(t, int32(0), target.Checks[0].Check.GetF().Args[0].GetCol().ColPos)
	hiddenAndMoved := &planpb.TableDef{Name: "copy", Cols: []*planpb.ColDef{
		{ColId: 2, Name: "b", Typ: decimal},
		{ColId: 3, Name: "q", Typ: decimal, GeneratedCol: &planpb.GeneratedCol{Expr: division(4, 0)}},
		{ColId: 5, Name: "__mo_hidden", Hidden: true},
		{ColId: 4, Name: "note", Typ: planpb.Type{Id: 6}},
		{ColId: 1, Name: "a", Typ: decimal},
	}, Checks: []*planpb.CheckDef{{Name: "valid", Check: division(0, 1)}}}
	replay = ddlReplayForTable(WithPersistedDDLReplay(context.Background(), old, hiddenAndMoved), "copy")
	require.Equal(t, int32(3), replay.columns["q"].generated.Expr.GetF().Args[0].GetCol().ColPos)
	require.Equal(t, int32(0), replay.columns["q"].generated.Expr.GetF().Args[1].GetCol().ColPos)
	require.Equal(t, int32(3), replay.checks["valid"].Check.GetF().Args[0].GetCol().ColPos)

	changedType := DeepCopyTableDef(target, true)
	changedType.Cols[2].Typ.Scale = 3
	replay = ddlReplayForTable(WithPersistedDDLReplay(context.Background(), old, changedType), "copy")
	require.Nil(t, replay.columns["q"].generated)
	require.Nil(t, replay.checks["valid"])

	changedEnum := DeepCopyTableDef(target, true)
	changedEnum.Cols[2].Typ.Enumvalues = "b,a"
	replay = ddlReplayForTable(WithPersistedDDLReplay(context.Background(), old, changedEnum), "copy")
	require.Nil(t, replay.columns["q"].generated)
	require.Nil(t, replay.checks["valid"])

	changedAutoIncrement := DeepCopyTableDef(target, true)
	changedAutoIncrement.Cols[2].Typ.AutoIncr = true
	replay = ddlReplayForTable(WithPersistedDDLReplay(context.Background(), old, changedAutoIncrement), "copy")
	require.Nil(t, replay.columns["q"].generated)
	require.Nil(t, replay.checks["valid"])

	changedExpr := DeepCopyTableDef(target, true)
	changedExpr.Cols[3].GeneratedCol.Expr = ref(1)
	replay = ddlReplayForTable(WithPersistedDDLReplay(context.Background(), old, changedExpr), "copy")
	require.Nil(t, replay.columns["q"].generated)
	require.Nil(t, ddlReplayForTable(WithPersistedDDLReplay(context.Background(), old, target), "other"))
}
