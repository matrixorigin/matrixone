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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// Exercise admission through the receiver before any pooled operator is built.
func TestDecimalStatisticalDescriptorReceive(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	double := types.T_float64.ToType().PlanType()
	decimal := types.New(types.T_decimal128, 38, 18).PlanType()
	integer := types.T_int64.ToType().PlanType()
	for _, op := range []int32{int32(vm.Group), int32(vm.MergeGroup)} {
		for _, nested := range []bool{false, true} {
			for _, id := range []int64{aggexec.AggIdOfVarPop, aggexec.AggIdOfVarSample, aggexec.AggIdOfStdDevPop, aggexec.AggIdOfStdDevSample} {
				for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_float64, types.T_int64} {
					t.Run(fmt.Sprintf("op%d/nested%t/agg%d/%s", op, nested, id, oid), func(t *testing.T) {
						affected := oid == types.T_decimal64 || oid == types.T_decimal128
						wire := &pipeline.Aggregate{Op: id, Dist: true, Expr: []*planpb.Expr{{Typ: oid.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}}
						leaf := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{Op: op, Agg: &pipeline.Group{NeedEval: true, Aggs: []*pipeline.Aggregate{wire}}}}}
						p := leaf
						if nested {
							p = &pipeline.Pipeline{Children: []*pipeline.Pipeline{leaf}}
						}
						if affected {
							wire.ResultType = &double
						}
						data, err := p.Marshal()
						require.NoError(t, err)
						scope, decodeErr := decodeScope(data, c.proc, true, nil)
						if scope != nil {
							t.Cleanup(scope.release)
						}
						require.NoError(t, decodeErr)
						require.NotNil(t, scope)
						if !affected {
							return
						}
						for _, contract := range []*planpb.Type{nil, &decimal, &integer} {
							wire.ResultType = contract
							data, err = p.Marshal()
							require.NoError(t, err)
							scope, decodeErr := decodeScope(data, c.proc, true, nil)
							require.ErrorContains(t, decodeErr, "rebinding", "legacy or incompatible result ABI")
							require.Nil(t, scope, "reject before constructing operator owners")
						}
					})
				}
			}
		}
	}
}
