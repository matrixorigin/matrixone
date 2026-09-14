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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestHexCatalogMigrationPreservesColumnMetadata(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, present := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion70)
	t.Cleanup(func() {
		if present {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion70)
		}
	})
	for _, overload := range []int32{5, function.HexFloat64Overload} {
		for _, wrapper := range []string{"default", "generated", "on_update"} {
			t.Run(fmt.Sprintf("%s_%d", wrapper, overload), func(t *testing.T) {
				expr := &pb.Expr{Typ: pb.Type{Id: int32(types.T_varchar)}, Expr: &pb.Expr_F{F: &pb.Function{
					Func: &pb.ObjectRef{Obj: function.EncodeOverloadID(function.HEX, overload), ObjName: "hex"},
					Args: []*pb.Expr{MakePlan2Float64ConstExprWithType(14.5)},
				}}}
				col := &pb.ColDef{ColId: 42, Name: "h", OriginName: "H", Typ: expr.Typ,
					NotNull: true, LowCard: true, Headers: true, Header: "header-key",
					Hidden: true, Comment: "keep", Seqnum: 7, ClusterBy: true, Primary: true,
					Pkidx: 2, TblName: "t", DbName: "db", Unique: true, OriginTblName: "source"}
				switch wrapper {
				case "default":
					col.Default = &pb.Default{Expr: expr, OriginString: "hex(x)", NullAbility: true}
				case "generated":
					col.GeneratedCol = &pb.GeneratedCol{Expr: expr, OriginString: "hex(x)", IsStored: true}
				case "on_update":
					col.OnUpdate = &pb.OnUpdate{Expr: expr, OriginString: "hex(x)"}
				}
				before, err := col.Marshal()
				require.NoError(t, err)
				// Build the independent expected value through protobuf, not the
				// potentially incomplete copy helper under test.
				want := new(pb.ColDef)
				require.NoError(t, want.Unmarshal(before))
				var wantExpr *pb.Expr
				switch wrapper {
				case "default":
					wantExpr = want.Default.Expr
				case "generated":
					wantExpr = want.GeneratedCol.Expr
				case "on_update":
					wantExpr = want.OnUpdate.Expr
				}
				wantExpr.GetF().Func.Obj = function.EncodeOverloadID(function.HEX, function.HexFloat64Overload)
				catalog := &pb.TableDef{Cols: []*pb.ColDef{col}}
				owned := CloneTableDefForPlan(catalog, true)
				for range 2 {
					require.NoError(t, MigrateLegacyHexTableDef(proc, owned))
					require.Equal(t, want, owned.Cols[0], "only the target overload may change")
					after, err := col.Marshal()
					require.NoError(t, err)
					require.Equal(t, before, after, "catalog must remain unchanged")
				}
			})
		}
	}
}

func TestHexCatalogMigrationOwnsAllExpressionWrappers(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, present := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion70)
	t.Cleanup(func() {
		if present {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion70)
		}
	})
	legacy := &pb.Expr{Typ: pb.Type{Id: int32(types.T_varchar)}, Expr: &pb.Expr_F{F: &pb.Function{
		Func: &pb.ObjectRef{Obj: function.EncodeOverloadID(function.HEX, 5), ObjName: "hex"},
		Args: []*pb.Expr{MakePlan2Float64ConstExprWithType(14.5)},
	}}}
	// Sharing the expression between wrappers is intentional: even intra-catalog
	// aliases must not become writable through a resolver's shallow clone.
	catalog := &pb.TableDef{Checks: []*pb.CheckDef{{Check: legacy}}, Cols: []*pb.ColDef{
		{Name: "d", Default: &pb.Default{Expr: legacy}},
		{Name: "g", GeneratedCol: &pb.GeneratedCol{Expr: legacy}},
		{Name: "u", OnUpdate: &pb.OnUpdate{Expr: legacy}},
		{Name: "control", Default: &pb.Default{Expr: MakePlan2Int64ConstExprWithType(1)}},
	}}
	wire, err := catalog.Marshal()
	require.NoError(t, err)
	expressions := func(table *pb.TableDef) []*pb.Expr {
		return []*pb.Expr{table.Checks[0].Check, table.Cols[0].Default.Expr,
			table.Cols[1].GeneratedCol.Expr, table.Cols[2].OnUpdate.Expr}
	}
	for worker := range 4 {
		t.Run(fmt.Sprintf("resolver_%d", worker), func(t *testing.T) {
			t.Parallel()
			owned := CloneTableDefForPlan(catalog, true)
			require.Same(t, catalog.Checks[0], owned.Checks[0])
			require.Same(t, catalog.Cols[0], owned.Cols[0])
			for range 2 {
				require.NoError(t, MigrateLegacyHexTableDef(proc, owned))
				for _, expr := range expressions(owned) {
					_, overload := function.DecodeOverloadID(expr.GetF().Func.Obj)
					require.Equal(t, int32(function.HexFloat64Overload), overload)
				}
				for _, expr := range expressions(catalog) {
					_, overload := function.DecodeOverloadID(expr.GetF().Func.Obj)
					require.Equal(t, int32(5), overload)
				}
			}
			require.NotSame(t, catalog.Checks[0], owned.Checks[0])
			require.NotSame(t, catalog.Cols[0].Default, owned.Cols[0].Default)
			require.NotSame(t, catalog.Cols[1].GeneratedCol, owned.Cols[1].GeneratedCol)
			require.NotSame(t, catalog.Cols[2].OnUpdate, owned.Cols[2].OnUpdate)
			require.Same(t, catalog.Cols[3], owned.Cols[3], "no-HEX control must remain shared")
			after, err := catalog.Marshal()
			require.NoError(t, err)
			require.Equal(t, wire, after)
		})
	}
}
