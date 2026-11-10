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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestRemoteBoundStringVariableFoldRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		charset uint8
		domain  types.RuntimeStringDomain
		binary  bool
	}{
		{"text over binary static type", types.CharsetBinary, types.RuntimeStringText, false},
		{"binary over text static type", types.CharsetUTF8MB4Bin, types.RuntimeStringBinary, true},
		{"inherit binary static type", types.CharsetBinary, types.RuntimeStringInherit, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCLatestVersion
			value := "你a"
			c.proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return value, nil })
			c.proc.SetResolveVariableStringDomainFunc(func(string, bool, bool) (types.RuntimeStringDomain, error) {
				return types.RuntimeStringInherit, moerr.NewInternalErrorNoCtx("do not re-resolve a bound domain")
			})
			typ := types.NewWithCharset(types.T_varchar, 8, 0, tc.charset)
			variable := makeTestVarExprWithType("bound_s", typ)
			variable.GetV().System = false
			variable.GetV().BoundStringDomain = uint32(tc.domain) + 1
			witness := makeTestVarExprWithType("bound_s", typ)
			witness.GetV().System = false
			witness.GetV().BoundStringDomain = variable.GetV().BoundStringDomain
			variable.PreparedNumeric = &plan.PreparedNumericMetadata{StringDomainSource: witness}
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = []*plan.Expr{variable}
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			_, err := encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "must be folded")
			_, err = encodeScope(scope)
			require.ErrorContains(t, err, "must be folded")

			remote := testutil.NewProcess(t) // No access to the source session.
			for _, next := range []string{"你a", "a你"} {
				value = next
				folded, changed, err := foldVarExprsInRemoteRunScope(scope, c.proc)
				require.NoError(t, err)
				require.True(t, changed)
				payload, err := encodeRemoteScope(folded, c.proc)
				require.NoError(t, err)
				var wire pipeline.Pipeline
				require.NoError(t, wire.Unmarshal(payload))
				require.NoError(t, validateRemoteBoundStringVariables(&wire))
				expr := wire.InstructionList[0].ProjectList[0]
				require.Equal(t, variable.Typ, expr.Typ)
				require.Nil(t, expr.GetV())
				require.True(t, plan.HasBoundStringVariable(expr), "retained planning witnesses are not executable variables")
				func() {
					executor, err := colexec.NewExpressionExecutor(remote, expr)
					require.NoError(t, err)
					defer executor.Free()
					v, err := executor.Eval(remote, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					require.Equal(t, next, v.GetStringAt(0))
					require.Equal(t, tc.binary, v.GetIsBinaryStringAt(0))
					require.Equal(t, tc.charset, v.GetType().Charset)
					require.Equal(t, types.StringSourceUserVariable, v.GetStringSourceAt(0))
				}()
				require.Zero(t, remote.Mp().CurrNB())
				require.Equal(t, uint32(tc.domain)+1, variable.GetV().BoundStringDomain)
				require.Same(t, variable, op.ProjectList[0], "private folding must not overwrite the reusable plan")
			}
		})
	}
}

func TestRemoteBoundStringVariableAdmissionOwners(t *testing.T) {
	proc := testutil.NewProcess(t)
	variable := makeTestVarExprWithType("s", types.T_text.ToType())
	variable.GetV().System = false
	variable.GetV().BoundStringDomain = 1
	for _, p := range []*pipeline.Pipeline{
		{InstructionList: []*pipeline.Instruction{{Op: int32(vm.Projection), ProjectList: []*plan.Expr{variable}}}},
		{DataSource: &pipeline.Source{Expr: variable}},
		{DataSource: &pipeline.Source{RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{Expr: variable}}}},
		{DataSource: &pipeline.Source{Node: &plan.Node{BlockFilterList: []*plan.Expr{variable}}}},
		{DataSource: &pipeline.Source{Node: &plan.Node{IndexReaderParam: &plan.IndexReaderParam{Limit: variable}}}},
		{DataSource: &pipeline.Source{Node: &plan.Node{IndexSearchScan: &plan.IndexSearchScan{QueryPayload: variable}}}},
		{DataSource: &pipeline.Source{TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Default: &plan.Default{Expr: variable}}}}}},
		{DataSource: &pipeline.Source{Node: &plan.Node{TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Default: &plan.Default{Expr: variable}}}}}}},
	} {
		require.ErrorContains(t, validateRemoteBoundStringVariables(p), "must be folded")
		require.ErrorContains(t, validateRemoteBoundStringVariables(&pipeline.Pipeline{Children: []*pipeline.Pipeline{p}}), "must be folded")
		payload, err := p.Marshal()
		require.NoError(t, err)
		decoded, err := decodeScope(payload, proc, true, nil)
		require.Nil(t, decoded)
		require.ErrorContains(t, err, "must be folded")
	}
	require.NoError(t, validateRemoteBoundStringVariables(nil))
	// The retained query is diagnostic/planning metadata, not a request for
	// the worker to evaluate a variable. Its executable projection is folded.
	p := &pipeline.Pipeline{Qry: &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{variable}}}}}}}
	require.NoError(t, validateRemoteBoundStringVariables(p))
	// The binder can retain a compact variable witness on a flattened scalar
	// subquery's ColRef. It is used for specialization, never worker evaluation.
	p.InstructionList = []*pipeline.Instruction{{Op: int32(vm.Projection), ProjectList: []*plan.Expr{{
		Typ:             variable.Typ,
		Expr:            &plan.Expr_Col{Col: &plan.ColRef{}},
		PreparedNumeric: &plan.PreparedNumericMetadata{StringDomainSource: variable},
	}}}}
	require.NoError(t, validateRemoteBoundStringVariables(p))
	require.True(t, plan.HasBoundStringVariable(p), "migration must still see the retained binding")
	variable.GetV().BoundStringDomain = 0
	require.NoError(t, validateRemoteBoundStringVariables(&pipeline.Pipeline{DataSource: &pipeline.Source{Expr: variable}}))
}
