// Copyright 2021 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSchemaEvolutionAggregateOracle(t *testing.T) {
	ctx := t.Context()
	fs, err := fileservice.NewLocalFS(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.FileService = fs
	c := &Compile{proc: proc}
	mp := proc.Mp()
	flagType := types.T_bool.ToType()
	idType := types.T_int64.ToType()
	def := &plan.TableDef{FeatureFlag: features.SchemaEvolved, Cols: []*plan.ColDef{
		{Name: "id", Typ: idType.PlanType()},
		{Name: "flag", Seqnum: 1, Typ: flagType.PlanType(), Default: &plan.Default{
			NullAbility: true, Expr: plan2.MakePlan2BoolConstExprWithType(true),
		}},
	}}
	flag := &plan.Expr{Typ: flagType.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: "flag"}}}
	node := &plan.Node{TableDef: def}
	for _, name := range []string{"starcount", "count", "min", "max"} {
		node.AggList = append(node.AggList, &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name}, Args: []*plan.Expr{flag},
		}}})
	}
	write := func(id int64, hasFlag, isNull bool) objectio.BlockInfo {
		t.Helper()
		in := batch.NewWithSize(1)
		in.Vecs[0] = vector.NewVec(idType, mp)
		defer in.Clean(mp)
		require.NoError(t, vector.AppendFixed(in.Vecs[0], id, false, mp))
		seqs := []uint16{0}
		if hasFlag {
			in.Vecs = append(in.Vecs, vector.NewVec(flagType, mp))
			require.NoError(t, vector.AppendFixed(in.Vecs[1], true, isNull, mp))
			seqs = append(seqs, 1)
		}
		in.SetRowCount(1)
		writer := ioutil.ConstructWriter(0, seqs, -1, false, false, fs)
		_, err := writer.WriteBatch(in)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		stats := writer.GetObjectStats()
		return stats.ConstructBlockInfo(0)
	}
	old, newTrue, newNull := write(1, false, false), write(3, true, false), write(4, true, true)
	results, typs, colMap := checkAggOptimize(node)
	spec := objectio.NewColumnReadSpecFromTableDef(def)
	require.NoError(t, c.evalAggOptimize(node, &old, results, typs, colMap, spec))
	require.Equal(t, []any{int64(1), int64(0), nil, nil}, results)
	require.NoError(t, c.evalAggOptimize(node, &newNull, results, typs, colMap, spec))
	require.Equal(t, []any{int64(2), int64(0), nil, nil}, results)
	require.NoError(t, c.evalAggOptimize(node, &newTrue, results, typs, colMap, spec))
	require.Equal(t, []any{int64(3), int64(1), true, true}, results)
	require.Equal(t, []types.T{types.T_int64, types.T_int64, types.T_bool, types.T_bool}, typs)
}
