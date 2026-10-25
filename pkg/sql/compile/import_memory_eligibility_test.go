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
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/stretchr/testify/require"
)

func TestImportMemoryRemoteTransportKeepsWholeStatementLegacy(t *testing.T) {
	c := memoryCompileFixture(t, 8)
	remoteA := memorySourceScope("127.0.0.2:6001", 3, 1)
	remoteB := memorySourceScope("127.0.0.2:6001", 5, 4)
	for _, scope := range []*Scope{remoteA, remoteB} {
		scope.Magic = Remote
		scope.Proc = c.proc
	}
	c.scopes = append(c.scopes, remoteA, remoteB)
	manifest, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	require.Empty(t, manifest.Stages, "remote queue/fragment/decode owners have no complete transport promise")
	m := &compilerMemoryAdmission{failure: errors.New("unsupported transport must not open")}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Zero(t, m.tries)
	require.Nil(t, c.importMemory)
	require.Nil(t, c.proc.Base.ImportMemory)
	c.endImportMemory()
}

func TestImportMemoryUnsupportedImportWritersKeepWholeStatementLegacy(t *testing.T) {
	cases := map[string]vm.Operator{
		"table-insert":            &insert.Insert{InsertCtx: &insert.InsertCtx{Engine: &memoryengine.BindedEngine{}}},
		"table-multi-update":      &multi_update.MultiUpdate{Action: multi_update.UpdateWriteTable},
		"partition-insert":        insert.NewPartitionInsert(&insert.Insert{ToWriteS3: true}, 1),
		"partition-multi-update":  multi_update.NewPartitionMultiUpdate(&multi_update.MultiUpdate{Action: multi_update.UpdateWriteS3}, 1),
		"memory-table-raw-output": &insert.Insert{ToWriteS3: true, InsertCtx: &insert.InsertCtx{Engine: &memoryengine.BindedEngine{}}},
		"derived-composite-key":   &preinsert.PreInsert{CompPkeyExpr: &plan.Expr{}},
		"derived-cluster-key":     &preinsert.PreInsert{ClusterByExpr: &plan.Expr{}},
	}
	for name, op := range cases {
		t.Run(name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			writer := &Scope{Proc: c.proc, NodeInfo: engine.Node{Addr: c.addr, Mcpu: 1}, RootOp: op}
			c.scopes[0].PreScopes = []*Scope{writer}
			m := &compilerMemoryAdmission{failure: errors.New("admission must not be attempted")}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m), "unsupported nested writers must select legacy before any admission")
			require.Zero(t, m.tries)

			require.Nil(t, c.importMemory)
			require.Nil(t, c.proc.Base.ImportMemory)
		})
	}
}

func TestImportMemoryPlainInsertRemainsControlledOnlyWithBoundedSource(t *testing.T) {
	table := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}, {Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar), Width: 1024}}}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}}
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprint(source), func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			if !source {
				c.scopes = nil
			}
			pre := &preinsert.PreInsert{TableDef: table}
			writer := &insert.Insert{ToWriteS3: true, InsertCtx: &insert.InsertCtx{TableDef: table}}
			writer.AppendChild(pre)
			c.scopes = append(c.scopes, &Scope{NodeInfo: engine.Node{Addr: c.addr, Mcpu: 1}, RootOp: writer})
			sentinel := errors.New("bounded plain insert reached admission")
			m := &compilerMemoryAdmission{failure: sentinel}
			err := c.beginStatementImportMemory(context.Background(), m)
			if source {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, 1, m.tries, "ordinary source + raw single PK writer must remain eligible")
			} else {
				require.NoError(t, err)
				require.Zero(t, m.tries, "a table writer alone has no bounded CSV input contract")
			}
			require.Nil(t, c.importMemory)
		})
	}
}

func TestImportMemoryUnknownVarlenKeysKeepWholeLoadLegacy(t *testing.T) {
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	unknown := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "unknown_varlen_result"}, Args: []*plan.Expr{col}}}}
	cases := map[string]vm.Operator{
		"build":                   &hashbuild.HashBuild{Conditions: []*plan.Expr{unknown}},
		"probe-left":              &hashjoin.HashJoin{EqConds: [][]*plan.Expr{{unknown}, {col}}},
		"probe-right":             &hashjoin.HashJoin{EqConds: [][]*plan.Expr{{col}, {unknown}}},
		"unknown-fixed-predicate": &hashjoin.HashJoin{EqConds: [][]*plan.Expr{{col}, {col}}, NonEqCond: &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "json_valid"}, Args: []*plan.Expr{col}}}}},
	}
	for name, op := range cases {
		t.Run(name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			c.scopes = append(c.scopes, &Scope{NodeInfo: engine.Node{Addr: c.addr, Mcpu: 1}, RootOp: op})
			m := &compilerMemoryAdmission{available: 0}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
			require.Zero(t, m.tries, "no I/W ticket may coexist with a build whose key result has no allocation bound")
			require.Nil(t, c.importMemory)
		})
	}
	c := memoryCompileFixture(t, 1)
	serial := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varbinary)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "serial_full"}, Args: []*plan.Expr{col}}}}
	c.scopes = append(c.scopes, &Scope{RootOp: &hashbuild.HashBuild{Conditions: []*plan.Expr{serial}}})
	require.True(t, c.supportsBoundedImportKeys(), "known serialization keys retain the controlled LOAD path")
}

func TestImportMemoryManifestPreservesBoundedKeyExpressionTrees(t *testing.T) {
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	serial := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varbinary)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "serial_full"}, Args: []*plan.Expr{col, col}}}}
	join := &hashjoin.HashJoin{LeftTypes: []types.Type{types.T_varchar.ToType()}, RightTypes: []types.Type{types.T_varchar.ToType()}, EqConds: [][]*plan.Expr{{col}, {serial}}}
	stage, controlled, err := memoryStage(join, 2)
	require.NoError(t, err)
	require.True(t, controlled)
	manifest := MemoryManifest{TargetUnit: 1 << 20, Parallel: 2, Stages: []MemoryStageDescriptor{stage}}
	before, err := serial.Marshal()
	require.NoError(t, err)
	local, spec, controlled, err := memoryManifestLayout(manifest, 1<<20)
	require.NoError(t, err)
	require.True(t, controlled)
	require.GreaterOrEqual(t, local.IOBytes, local.JoinInputBytes, "key evaluation and frame compaction need a pre-reserved workspace")
	require.Equal(t, uint32(4), spec.Inputs[2].Count, "both join inputs have a slot for each physical copy")
	expression := manifest.Stages[0].KeyExpressions[1][0]
	after, err := serial.Marshal()
	require.NoError(t, err)
	require.Equal(t, before, after, "bound calculation must not mutate the local plan")
	require.Equal(t, "serial_full", expression.GetF().Func.ObjName)
	require.Len(t, expression.GetF().Args, 2, "repeated source columns must survive manifest construction for memory amplification accounting")
}

func TestImportMemoryCapacityEnvelopeShrinksBeforeAdmission(t *testing.T) {
	key := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	for depth := 0; depth < 5; depth++ {
		key = &plan.Expr{Typ: plan.Type{Id: int32(types.T_varbinary)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "serial_full"}, Args: []*plan.Expr{key}}}}
	}
	join := &hashjoin.HashJoin{LeftTypes: []types.Type{types.T_varchar.ToType()}, RightTypes: []types.Type{types.T_varchar.ToType()}, EqConds: [][]*plan.Expr{{key}, {key}}}
	stage, ok, err := memoryStage(join, 1)
	require.NoError(t, err)
	require.True(t, ok)
	manifest := MemoryManifest{TargetUnit: 64 << 20, Parallel: 1, Stages: []MemoryStageDescriptor{stage}}
	_, _, _, err = memoryManifestLayout(manifest, 64<<20)
	var capacity *memoryEnvelopeCapacityError
	require.ErrorAs(t, err, &capacity, "large serialized keys can exceed vector capacity at the initial unit")
	_, _, _, err = memoryManifestLayout(manifest, 32<<20)
	require.NoError(t, err, "the same legal expression has a smaller calculable unit")
	reached := errors.New("reached atomic admission")
	provider := &compilerMemoryAdmission{failure: reached}
	_, err = admitMemoryManifest(context.Background(), provider, manifest)
	require.ErrorIs(t, err, reached)
	require.Equal(t, int64(32<<20), provider.lastUnit)
	require.Equal(t, 1, provider.tries, "no resources are requested until the envelope can be computed")
}

// OpType inspection marks entry to producer analysis. Ordinary source discovery
// only follows the operator tree, so it must leave this counter untouched.
type importSourceGateOperator struct {
	vm.Operator
	inspections int
}

func (op *importSourceGateOperator) OpType() vm.OpType {
	op.inspections++
	return vm.Output
}

func TestImportMemoryNoSourceLeavesBeforeProducerAnalysis(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	source := c.scopes[0].RootOp
	op := &importSourceGateOperator{Operator: colexec.NewMockOperator()}
	c.scopes[0].RootOp = op
	failure := errors.New("controlled source reached admission")
	budget := &compilerMemoryAdmission{failure: failure}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), budget))
	require.Zero(t, op.inspections)
	require.Zero(t, budget.tries)
	op.GetOperatorBase().AppendChild(source)
	require.ErrorIs(t, c.beginStatementImportMemory(context.Background(), budget), failure)
	require.Positive(t, op.inspections, "a candidate import still needs the complete producer proof")
	require.Equal(t, 1, budget.tries)
}
