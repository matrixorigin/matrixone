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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestVirtualTimestampReadAndIndexEligibility(t *testing.T) {
	ref := func(pos int32, typ types.T) *pb.Expr {
		return &pb.Expr{Typ: pb.Type{Id: int32(typ)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos}}}
	}
	table := &pb.TableDef{
		Cols: []*pb.ColDef{
			{Name: "ts", Typ: pb.Type{Id: int32(types.T_timestamp)}},
			{Name: "d", Typ: pb.Type{Id: int32(types.T_date)}, GeneratedCol: &pb.GeneratedCol{Expr: ref(0, types.T_date)}},
			{Name: "child", Typ: pb.Type{Id: int32(types.T_date)}, GeneratedCol: &pb.GeneratedCol{Expr: ref(1, types.T_date)}},
			{Name: "stored", Typ: pb.Type{Id: int32(types.T_date)}, GeneratedCol: &pb.GeneratedCol{Expr: ref(0, types.T_date), IsStored: true}},
			{Name: "stable", Typ: pb.Type{Id: int32(types.T_date)}, GeneratedCol: &pb.GeneratedCol{Expr: ref(3, types.T_date)}},
		},
		Name2ColIndex: map[string]int32{"ts": 0, "d": 1, "child": 2, "stored": 3, "stable": 4},
	}
	require.True(t, virtualTimestampColumn(table, 1))
	require.True(t, virtualTimestampColumn(table, 2))
	require.False(t, virtualTimestampColumn(table, 3))
	require.False(t, virtualTimestampColumn(table, 4))
	require.True(t, indexUsesVirtualTimestamp(&pb.IndexDef{Parts: []string{"stored", "child"}}, table))
	require.False(t, indexUsesVirtualTimestamp(&pb.IndexDef{Parts: []string{"stable"}}, table))

	bound := expandVirtualTimestampColumn(table, 2, 17, 0, make(map[int32]bool))
	require.NotNil(t, bound)
	require.Equal(t, int32(17), bound.GetCol().RelPos)
	require.Equal(t, int32(0), bound.GetCol().ColPos)
	correlated := expandVirtualTimestampColumn(table, 2, 17, 1, make(map[int32]bool))
	require.NotNil(t, correlated)
	require.Equal(t, int32(17), correlated.GetCorr().RelPos)
	require.Equal(t, int32(0), correlated.GetCorr().ColPos)
	require.Equal(t, int32(1), correlated.GetCorr().Depth)
}

func TestVirtualTimestampClassificationSharedDependencies(t *testing.T) {
	table := &pb.TableDef{
		Cols:          []*pb.ColDef{{Name: "x", Typ: pb.Type{Id: int32(types.T_int32)}}},
		Name2ColIndex: map[string]int32{"x": 0},
	}
	for level := 1; level <= 30; level++ {
		name := fmt.Sprintf("g%d", level)
		previous := &pb.Expr{Typ: pb.Type{Id: int32(types.T_int32)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: int32(level - 1)}}}
		expr := &pb.Expr{Typ: pb.Type{Id: int32(types.T_int32)}, Expr: &pb.Expr_F{F: &pb.Function{Args: []*pb.Expr{previous, DeepCopyExpr(previous)}}}}
		table.Cols = append(table.Cols, &pb.ColDef{Name: name, Typ: expr.Typ, GeneratedCol: &pb.GeneratedCol{Expr: expr}})
		table.Name2ColIndex[name] = int32(level)
	}
	start := time.Now()
	require.False(t, indexUsesVirtualTimestamp(&pb.IndexDef{Parts: []string{"g30"}}, table))
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestVirtualTimestampExpansionAdmissionChecksDescendants(t *testing.T) {
	table := &pb.TableDef{Name: "dag", Cols: []*pb.ColDef{{Name: "source", Typ: pb.Type{Id: int32(types.T_datetime)}}}}
	ref := func(pos int32, typ types.T) *pb.Expr {
		return &pb.Expr{Typ: pb.Type{Id: int32(typ)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos}}}
	}
	table.Cols = append(table.Cols, &pb.ColDef{Name: "g0", Typ: pb.Type{Id: int32(types.T_date)},
		GeneratedCol: &pb.GeneratedCol{Expr: ref(0, types.T_date)}})
	for level := 1; level <= 16; level++ {
		previous := ref(int32(level), types.T_int32)
		table.Cols = append(table.Cols, &pb.ColDef{Name: fmt.Sprintf("g%d", level),
			Typ: pb.Type{Id: int32(types.T_int32)}, GeneratedCol: &pb.GeneratedCol{Expr: &pb.Expr{
				Typ:  pb.Type{Id: int32(types.T_int32)},
				Expr: &pb.Expr_F{F: &pb.Function{Args: []*pb.Expr{previous, DeepCopyExpr(previous)}}},
			}}})
	}
	require.Empty(t, VirtualTimestampExpansionFindings(table), "datetime source is session stable")
	table.Cols[0].Typ.Id = int32(types.T_timestamp)
	// g15 expands to exactly 65,535 surviving expression nodes. DDL
	// admission and runtime expansion must count replaced references alike.
	require.Equal(t, 65535, virtualTimestampReadCost(table, 16))
	require.NotNil(t, expandVirtualTimestampColumn(table, 16, 1, 0, make(map[int32]bool)))
	boundary := &pb.TableDef{Name: "boundary", Cols: append([]*pb.ColDef(nil), table.Cols[:17]...)}
	boundary.Cols = append(boundary.Cols, &pb.ColDef{Name: "exact", GeneratedCol: &pb.GeneratedCol{
		Expr: &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: []*pb.Expr{ref(16, types.T_int32)}}}},
	}})
	require.Equal(t, maxVirtualTimestampExpansionNodes, virtualTimestampReadCost(boundary, 17))
	require.Empty(t, VirtualTimestampExpansionFindings(boundary))
	require.NotNil(t, expandVirtualTimestampColumn(boundary, 17, 1, 0, make(map[int32]bool)))
	boundary.Cols = append(boundary.Cols, &pb.ColDef{Name: "over", GeneratedCol: &pb.GeneratedCol{
		Expr: &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: []*pb.Expr{ref(17, types.T_int32)}}}},
	}})
	require.Equal(t, maxVirtualTimestampExpansionNodes+1, virtualTimestampReadCost(boundary, 18))
	require.Equal(t, "over", VirtualTimestampExpansionFindings(boundary)[0].Column)
	require.Nil(t, expandVirtualTimestampColumn(boundary, 18, 1, 0, make(map[int32]bool)))
	findings := VirtualTimestampExpansionFindings(table)
	require.Len(t, findings, 1)
	require.Equal(t, "g16", findings[0].Column)
	require.Equal(t, maxVirtualTimestampExpansionNodes+1, findings[0].Cost)
	require.ErrorContains(t, validateVirtualTimestampExpansion(t.Context(), table), "g16")
	require.Equal(t, maxVirtualTimestampExpansionNodes, saturatingVirtualTimestampCost(maxVirtualTimestampExpansionNodes-1, 1))
	require.Equal(t, maxVirtualTimestampExpansionNodes+1, saturatingVirtualTimestampCost(maxVirtualTimestampExpansionNodes, 1))
}
