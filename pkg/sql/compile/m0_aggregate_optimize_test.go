//go:build mo_m0

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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

type m0AggregateTestRelation struct {
	engine.Relation
	extra         *api.SchemaExtra
	estimateCalls int
	starCalls     int
}

func (r *m0AggregateTestRelation) GetExtraInfo() *api.SchemaExtra { return r.extra }
func (r *m0AggregateTestRelation) EstimateCommittedTombstoneCount(context.Context) (int, error) {
	r.estimateCalls++
	return 0, nil
}
func (r *m0AggregateTestRelation) StarCount(context.Context) (uint64, error) {
	r.starCalls++
	return 3, nil
}

func TestM0OrdinaryStarCountUsesRelation(t *testing.T) {
	node := zmAggNode([]*plan.ColDef{zmAggColDef("v", 0, types.T_int32, false)}, zmAggFuncExpr("starcount", zmAggConstExpr(), 0))
	scan := &table_scan.TableScan{}
	partial := &group.Group{}
	partial.GetOperatorBase().AppendChild(scan)
	merge := &group.MergeGroup{}
	merge.GetOperatorBase().AppendChild(partial)
	data := readutil.NewBlockListRelationData(0)
	scope := &Scope{DataSource: &Source{node: node}, NodeInfo: engine.Node{Data: data}, RootOp: merge}
	rel := &m0AggregateTestRelation{}
	require.NoError(t, scope.aggOptimize(&Compile{}, rel, context.Background()))
	require.Equal(t, 1, rel.estimateCalls)
	require.Equal(t, 1, rel.starCalls)
	require.True(t, scope.StarCountOnly)
	require.Equal(t, []any{int64(3)}, merge.PartialResults)
}

func TestM0OrdinaryMetadataAggregateEligibility(t *testing.T) {
	for _, name := range []string{"starcount", "count", "min", "max"} {
		t.Run(name, func(t *testing.T) {
			arg := zmAggColExpr(0)
			if name == "starcount" {
				arg = zmAggConstExpr()
			}
			node := zmAggNode([]*plan.ColDef{zmAggColDef("v", 0, types.T_int32, false)}, zmAggFuncExpr(name, arg, 0))
			partial, _, _ := checkAggOptimize(node)
			require.NotNil(t, partial, "ordinary relations retain the eligible metadata path")
			require.True(t, metadataAggregateRelationAllowed(&m0AggregateTestRelation{}))
			if name == "starcount" {
				require.True(t, isSingleStarCountNoFilterNoGroupBy(node))
			}

			// A stale scan plan must not conceal a live relation contract.
			contract := &api.M0EvolutionContract{FormatVersion: 1}
			require.False(t, metadataAggregateRelationAllowed(&m0AggregateTestRelation{extra: &api.SchemaExtra{M0Evolution: contract}}))
			if name == "starcount" {
				scope := &Scope{DataSource: &Source{node: node}}
				rel := &m0AggregateTestRelation{extra: &api.SchemaExtra{M0Evolution: contract}}
				require.NoError(t, scope.aggOptimize(&Compile{}, rel, context.Background()))
				require.False(t, scope.StarCountOnly, "a stale plan must retain the normal reader")
				require.Zero(t, rel.starCalls)
			}
			node.TableDef.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: contract}))}}}}}}
			partial, _, _ = checkAggOptimize(node)
			require.Nil(t, partial, "an evolution carrier cannot use the ordinary metadata path")
			if name == "starcount" {
				require.False(t, isSingleStarCountNoFilterNoGroupBy(node))
			}
			node.TableDef.Defs[0].GetProperties().Properties[0].Value = "invalid protobuf"
			partial, _, _ = checkAggOptimize(node)
			require.Nil(t, partial, "a malformed carrier is not an ordinary relation")
		})
	}
}

func TestM0ValidEvolutionMetadataAggregateDeclined(t *testing.T) {
	def := &plan.TableDef{TblId: 42, DbId: 7, Version: 1, Cols: []*plan.ColDef{
		{ColId: 1, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
		{ColId: 2, Name: "b", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32)}},
		{ColId: 3, Name: catalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar)}},
		{ColId: 4, Name: "added", Seqnum: 3, Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
		{ColId: 5, Name: catalog.Row_ID, Seqnum: 4},
	}}
	def.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, CompPkeyCol: def.Cols[2], Names: []string{"a", "b"}}
	contract := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), AccountId: 42, DatabaseId: 7, RelationId: 42, RootColumns: 3, LayoutGeneration: 1}
	for i, col := range def.Cols[:4] {
		semantic, err := m0read.PlanEvolutionColumnSemantic(def, col)
		require.NoError(t, err)
		generation := uint32(0)
		if i == 3 {
			generation = 1
		}
		contract.Columns = append(contract.Columns, &api.M0EvolutionColumn{ColumnId: col.ColId, Seqnum: col.Seqnum, IntroducedLayout: generation, Semantics: semantic})
	}
	contract.SchemaDigest = m0read.EvolutionDigest(contract.Columns)
	def.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: contract}))}}}}}}
	_, err := m0read.ValidatePlanEvolution(def, 42)
	require.NoError(t, err)
	for _, name := range []string{"starcount", "count", "min", "max"} {
		arg := zmAggColExpr(3)
		if name == "starcount" {
			arg = zmAggConstExpr()
		}
		node := zmAggNode(def.Cols, zmAggFuncExpr(name, arg, 0))
		node.TableDef = def
		partial, _, _ := checkAggOptimize(node)
		require.Nil(t, partial, "%s must retain the strict M0 reader", name)
	}
}

func TestM0MetadataAggregateColumnBinding(t *testing.T) {
	cols := []*plan.ColDef{zmAggColDef("a", 0, types.T_int32, false), zmAggColDef("b", 1, types.T_int32, false)}
	col := func(name string) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: name, ColPos: 0}}}
	}
	node := zmAggNode(cols, zmAggFuncExpr("min", col("b"), 0))
	node.TableDef.Name2ColIndex = map[string]int32{"a": 0, "b": 1}
	partial, _, mapping := checkAggOptimize(node)
	require.NotNil(t, partial)
	require.Equal(t, map[int]int{0: 1}, mapping, "the evaluator uses the resolved physical sequence")

	node = zmAggNode(cols, zmAggFuncExpr("min", col("a"), 0), zmAggFuncExpr("max", col("b"), 0))
	node.TableDef.Name2ColIndex = map[string]int32{"a": 0, "b": 1}
	partial, _, _ = checkAggOptimize(node)
	require.Nil(t, partial, "ambiguous positions cannot share an aggregate metadata mapping")
}

func TestM0StarCountRequiresLiteral(t *testing.T) {
	cols := []*plan.ColDef{zmAggColDef("v", 0, types.T_int32, true)}
	for _, tc := range []struct {
		name string
		arg  *plan.Expr
	}{
		{"column", zmAggColExpr(0)},
		{"null literal", zmAggNullLitExpr()},
		{"missing literal value", &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := zmAggNode(cols, zmAggFuncExpr("starcount", tc.arg, 0))
			partial, _, _ := checkAggOptimize(node)
			require.Nil(t, partial)
			require.False(t, isSingleStarCountNoFilterNoGroupBy(node))
		})
	}
}

func TestM0MetadataAggregateRejectsMissingPhysicalColumn(t *testing.T) {
	proc := testutil.NewProcess(t)
	fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)
	bat := batch.NewWithSize(2)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[i], int32(7), false, proc.Mp()))
	}
	bat.SetRowCount(1)
	// A sparse object has metadata slots for sequence 0 and 2, but not 1.
	writer := ioutil.ConstructWriter(0, []uint16{0, 2}, 1, true, false, fs)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(proc.Ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	block := stats.ConstructBlockInfo(0)
	c := &Compile{proc: proc}
	for _, tc := range []struct {
		name    string
		seq     uint32
		typ     types.T
		missing bool
	}{{"present", 0, types.T_int32, false}, {"sparse hole", 1, types.T_int32, true}, {"past end", 3, types.T_int32, true}, {"wrong physical type", 0, types.T_int64, true}} {
		t.Run(tc.name, func(t *testing.T) {
			node := zmAggNode([]*plan.ColDef{zmAggColDef("v", tc.seq, tc.typ, false)}, zmAggFuncExpr("min", zmAggColExpr(0), 0))
			partial, resultTypes, columns := checkAggOptimize(node)
			require.NotNil(t, partial)
			err := c.evalAggOptimize(node, &block, partial, resultTypes, columns)
			if tc.missing {
				require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int32(7), partial[0])
		})
	}
}
