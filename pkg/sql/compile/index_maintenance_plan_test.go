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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

type maintenanceTestValidator struct{}

func (maintenanceTestValidator) ValidateIndexMaintenance(function string, args []string, spec *executor.IndexMaintenanceSpec) error {
	if function != spec.Function || len(args) < 2 || args[0] != "params" || args[1] != "config" {
		return moerr.NewInvalidInputNoCtx("unexpected maintenance literals")
	}
	return nil
}

func maintenanceTestSpec() *executor.IndexMaintenanceSpec {
	return &executor.IndexMaintenanceSpec{Function: "hnsw_create", AccountID: 0, Database: "db", DatabaseID: 7, BaseName: "base", BaseID: 8,
		Targets: []executor.IndexMaintenanceTarget{{Role: "meta", Name: "hidden", ID: 9}, {Role: "store", Name: "hidden2", ID: 10}}}
}

func TestIndexMaintenanceStatementAndPlanBinding(t *testing.T) {
	spec := maintenanceTestSpec()
	for _, tc := range []struct {
		sql  string
		pass bool
	}{
		{"select f.* from hnsw_create('params','config') as f", true},
		{"select * from hnsw_create('params','config') as f", true},
		{"select f.* from db.base as src cross apply hnsw_create('params','config',src.id) as f", true},
		{"select f.* from hnsw_create('params',concat('con','fig')) as f", false},
		{"select f.* from hnsw_create('params','config') as f, hnsw_create('params','config') as g", false},
		{"select f.* from db.other as src cross apply hnsw_create('params','config',src.id) as f", false},
		{"select f.* from hnsw_create('params','config') as f where 1=1", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			args, err := inspectIndexMaintenanceStatement(stmt, spec, maintenanceTestValidator{})
			if tc.pass {
				require.NoError(t, err)
				require.Equal(t, []string{"params", "config"}, args)
			} else {
				require.Error(t, err)
			}
		})
	}
	literal := func(s string) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: s}}}}
	}
	node := &plan.Node{NodeId: 0, NodeType: plan.Node_FUNCTION_SCAN, TableDef: &plan.TableDef{TblFunc: &plan.TableFunction{Name: "hnsw_create", Param: []byte("params")}}, TblFuncExprList: []*plan.Expr{literal("config")}}
	query := &plan.Query{Nodes: []*plan.Node{node}, Steps: []int32{0}}
	got, err := selectIndexMaintenanceNode(query, spec, []string{"params", "config"})
	require.NoError(t, err)
	require.Equal(t, int32(0), got)
	_, err = selectIndexMaintenanceNode(query, spec, []string{"params", "different"})
	require.Error(t, err)
	node.TblFuncExprList = []*plan.Expr{{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}}
	_, err = selectIndexMaintenanceNode(query, spec, []string{"params", "config"})
	require.Error(t, err)
}

func TestIndexMaintenanceViewProbeShape(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		pass bool
	}{
		{"select * from `db`.`view`", true},
		{"select * from `view`", false},
		{"select * from `db`.`view` where 1=1", false},
		{"select * from `db`.`view` as v", false},
		{"select v.* from `db`.`view` as v", false},
		{"select * from `db`.`view`, `db`.`other`", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			err = validateIndexMaintenanceViewProbe(stmt.(*tree.Select))
			if tc.pass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

type maintenanceTemporaryDelegate struct {
	sqlplan.CompilerContext
	def *plan.TableDef
}

func (d *maintenanceTemporaryDelegate) ResolveTemporaryTable(string, string, *sqlplan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	if d.def == nil {
		return nil, nil, nil
	}
	return &plan.ObjectRef{Obj: int64(d.def.TblId)}, d.def, nil
}

func TestIndexMaintenanceTemporaryDelegateGuard(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	delegate := &maintenanceTemporaryDelegate{CompilerContext: sqlplan.NewMockCompilerContext(false, proc)}
	c := &compilerContext{ctx: t.Context(), proc: proc, viewChild: true, viewDelegate: delegate, indexRelationPolicy: &executor.IndexRelationPolicy{}}
	// The missing temporary namespace remains a miss even for a protected name.
	obj, def, err := c.ResolveTemporaryTable("db", catalog.IndexTableNamePrefix+"missing", nil)
	require.NoError(t, err)
	require.Nil(t, obj)
	require.Nil(t, def)
	delegate.def = &plan.TableDef{TblId: 1, Name: "renamed", TableType: catalog.SystemTemporaryTable, FeatureFlag: features.IndexTable}
	_, _, err = c.ResolveTemporaryTable("db", "renamed", nil)
	require.Error(t, err)
	delegate.def.FeatureFlag = 0
	_, def, err = c.ResolveTemporaryTable("db", "renamed", nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), def.TblId)
	_, _, err = c.ResolveTemporaryTable("db", catalog.IndexTableNamePrefix+"child", nil)
	require.Error(t, err)
	c.indexRelationPolicy.CallerMayAccess = true
	delegate.def.FeatureFlag = features.IndexTable
	_, def, err = c.ResolveTemporaryTable("db", "renamed", nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), def.TblId)
}

func TestIndexMaintenanceCodecAndDuplication(t *testing.T) {
	spec := maintenanceTestSpec()
	wire, err := encodeIndexMaintenanceSpec(spec)
	require.NoError(t, err)
	data, err := wire.Marshal()
	require.NoError(t, err)
	var decoded pipeline.IndexMaintenanceSpec
	require.NoError(t, decoded.Unmarshal(data))
	got, err := decodeIndexMaintenanceSpec(&decoded)
	require.NoError(t, err)
	require.Equal(t, spec, got)
	got.Targets[0].Name = "changed"
	require.NotEqual(t, got.Targets[0].Name, spec.Targets[0].Name)
	decoded.Targets[0].Id = 0
	_, err = decodeIndexMaintenanceSpec(&decoded)
	require.Error(t, err)
	_, err = decodeIndexMaintenanceSpec(&pipeline.IndexMaintenanceSpec{Targets: make([]*pipeline.IndexMaintenanceTarget, 17)})
	require.Error(t, err)
	got, err = decodeIndexMaintenanceSpec(nil)
	require.NoError(t, err)
	require.Nil(t, got)

	tf := table_function.NewArgument()
	defer tf.Release()
	tf.FuncName, tf.IndexMaintenance = spec.Function, spec
	copy := dupOperator(tf, 0, 1).(*table_function.TableFunction)
	defer copy.Release()
	require.Equal(t, spec, copy.IndexMaintenance)
	copy.IndexMaintenance.Targets[0].Name = "changed"
	require.NotEqual(t, copy.IndexMaintenance.Targets[0].Name, spec.Targets[0].Name)
	applyOp := apply.NewArgument()
	defer applyOp.Release()
	applyOp.TableFunction = tf
	applyCopy := dupOperator(applyOp, 0, 1).(*apply.Apply)
	defer applyCopy.Release()
	require.Equal(t, spec, applyCopy.TableFunction.IndexMaintenance)
	applyCopy.TableFunction.Release()
	applyCopy.TableFunction = nil
	applyOp.TableFunction = nil
}

func TestIndexMaintenanceOperatorRemoteCodec(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctx := &scopeContext{id: 1}
	for _, applyMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "function", true: "apply"}[applyMode], func(t *testing.T) {
			tf := table_function.NewArgument()
			tf.FuncName = "hnsw_create"
			tf.IndexMaintenance = maintenanceTestSpec()
			var original vm.Operator = tf
			if applyMode {
				op := apply.NewArgument()
				op.TableFunction = tf
				original = op
			}
			defer original.Release()
			_, instruction, err := convertToPipelineInstruction(original, proc, ctx, 1)
			require.NoError(t, err)
			data, err := instruction.Marshal()
			require.NoError(t, err)
			var wire pipeline.Instruction
			require.NoError(t, wire.Unmarshal(data))
			restored, err := convertToVmOperator(&wire, ctx, nil)
			require.NoError(t, err)
			defer restored.Release()
			var restoredTF *table_function.TableFunction
			if applyMode {
				restoredTF = restored.(*apply.Apply).TableFunction
			} else {
				restoredTF = restored.(*table_function.TableFunction)
			}
			require.Equal(t, tf.IndexMaintenance, restoredTF.IndexMaintenance)
		})
	}
}
