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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// The SQL binder does not currently expose a known eager-safe reverse literal.
// Exercise the admitted typed plan all the way through the selector and the
// generated encoded-key filter and residual, independently of SQL parsing.
func TestTemporalCBOTimestampKeyTypedSelectionAndResidual(t *testing.T) {
	builder, source, _ := newIndexCostFixture(t)
	proc := builder.compCtx.GetProcess()
	scan := builder.qry.Nodes[0]
	scan.TableDef.Cols[0].Typ.Id = int32(types.T_timestamp)
	column := GetColExpr(scan.TableDef.Cols[0].Typ, scan.BindingTags[0], 0)
	column.GetCol().Name = "d"
	bound := types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0)
	literal := &pb.Expr{Typ: pb.Type{Id: int32(types.T_datetime), Scale: 6}, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Datetimeval{Datetimeval: int64(bound)}}}}
	filter, err := BindFuncExprImplByPlanExpr(proc.Ctx, ">", []*pb.Expr{column, literal})
	require.NoError(t, err)
	scan.FilterList = []*pb.Expr{filter}
	source.observation.Kind = types.T_timestamp
	source.observation.Min = float64(types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0).ToTimestamp(time.UTC))
	source.observation.Max = float64(bound.ToTimestamp(time.UTC) + 100*types.MicroSecsPerSec)
	steps := builder.qry.Steps
	builder.qry.Steps = nil
	builder.collectOriginalIndexOwner(0)
	builder.qry.Steps = steps
	require.True(t, builder.originalIndexScan.safe, "the original typed predicate must be admitted")
	id, err := builder.applyIndicesWithOrder(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, make(map[[2]int32]*pb.Expr), false)
	require.NoError(t, err)
	require.NotZero(t, id, "the typed path must actually select the covering index")
	selected := builder.qry.Nodes[id]
	require.True(t, selected.IndexScanInfo.IsIndexScan)
	require.Len(t, selected.BlockFilterList, 1)
	require.Len(t, selected.FilterList, 1)
	require.True(t, selected.BlockFilterList[0].CandidateFilter)
	require.False(t, selected.FilterList[0].CandidateFilter)

	// Encode exactly the index's (timestamp, primary-key) shape, then run the
	// chosen scan's own block expression and remapped complete row predicate.
	typedColumn := GetColExpr(scan.TableDef.Cols[0].Typ, 0, 0)
	serial, err := BindFuncExprImplByPlanExpr(proc.Ctx, indexTableComparisonSerialFunc(), []*pb.Expr{typedColumn, MakePlan2Int64ConstExprWithType(7)})
	require.NoError(t, err)
	encoder, err := colexec.NewExpressionExecutor(proc, serial)
	require.NoError(t, err)
	defer encoder.Free()
	raw := batch.NewWithSize(1)
	defer raw.Clean(proc.Mp())
	raw.Vecs[0] = vector.NewVec(types.New(types.T_timestamp, 0, 6))
	at := bound.ToTimestamp(time.UTC)
	values := []types.Timestamp{types.ZeroTimestamp, at - 1, at, at + 1, at + 1000000}
	require.NoError(t, vector.AppendFixedList(raw.Vecs[0], values, nil, proc.Mp()))
	raw.SetRowCount(len(values))
	encoded, err := encoder.Eval(proc, []*batch.Batch{raw}, nil)
	require.NoError(t, err)
	physical := batch.NewWithSize(1)
	physical.Vecs[0] = encoded // owned by encoder
	physical.SetRowCount(len(values))

	candidate := DeepCopyExpr(selected.BlockFilterList[0])
	residual := DeepCopyExpr(selected.FilterList[0])
	var toInput func(*pb.Expr)
	toInput = func(expr *pb.Expr) {
		if c := expr.GetCol(); c != nil {
			c.RelPos = 0
		}
		if f := expr.GetF(); f != nil {
			for _, arg := range f.Args {
				toInput(arg)
			}
		}
	}
	toInput(candidate)
	toInput(residual)
	candidateExec, err := colexec.NewExpressionExecutor(proc, candidate)
	require.NoError(t, err)
	defer candidateExec.Free()
	residualExec, err := colexec.NewExpressionExecutor(proc, residual)
	require.NoError(t, err)
	defer residualExec.Free()
	candidateResult, err := candidateExec.Eval(proc, []*batch.Batch{physical}, nil)
	require.NoError(t, err)
	residualResult, err := residualExec.Eval(proc, []*batch.Batch{physical}, nil)
	require.NoError(t, err)
	for i, value := range values {
		want := value > at
		gotCandidate := !candidateResult.IsNull(uint64(i)) && vector.GetFixedAtWithTypeCheck[bool](candidateResult, i)
		gotResidual := !residualResult.IsNull(uint64(i)) && vector.GetFixedAtWithTypeCheck[bool](residualResult, i)
		require.Equal(t, want, gotResidual, "residual row %d (%d)", i, value)
		require.Equal(t, want, gotCandidate && gotResidual, "physical row %d (%d)", i, value)
		if want {
			require.True(t, gotCandidate, "candidate omitted matching physical row %d", i)
		}
		if value == at-1 {
			require.False(t, gotCandidate, "a selective candidate must exclude an earlier key")
		}
	}
}
