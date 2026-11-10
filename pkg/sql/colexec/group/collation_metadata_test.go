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

package group

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMergeGroupExtraBufCollationAdmission(t *testing.T) {
	for _, revision := range []uint8{1, 2} {
		t.Run(fmt.Sprintf("revision_%d", revision), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			defer func() { require.Zero(t, proc.Mp().CurrNB()) }()
			typ := types.NewWithCharset(types.T_varchar, 8, 0, 3)
			input := vector.NewVec(typ)
			defer input.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(input, []byte("A"), false, proc.Mp()))
			ids := []int64{aggexec.AggIdOfMin, aggexec.AggIdOfMax}
			sources := make([]aggexec.AggFuncExec, 0, len(ids))
			expressions := make([]aggexec.AggFuncExecExpression, 0, len(ids))
			for _, id := range ids {
				source, err := aggexec.MakeAgg(proc.Mp(), id, false, typ)
				require.NoError(t, err)
				defer source.Free()
				require.NoError(t, source.GroupGrow(1))
				require.NoError(t, source.BulkFill(0, []*vector.Vector{input}))
				sources = append(sources, source)
				expr := colExpr(0, types.T_varchar)
				expr.Typ = typ.PlanType()
				expressions = append(expressions, aggexec.MakeAggFunctionExpression(id, false, []*plan.Expr{expr}, nil))
			}
			merge := newMergeGroupOp(expressions)
			defer merge.Free(proc, false, nil)
			require.NoError(t, merge.Prepare(proc))
			partial := batch.NewWithSize(0) // No ordinary columns for decodeBatch to inspect.
			partial.SetRowCount(1)
			defer partial.Clean(proc.Mp())
			for _, incomingRevision := range []uint8{revision, 0} {
				sources[1].(aggexec.PrepareParamKindStateAccessor).PrepareParamKindVectorForChunk(0).
					GetType().CollationVersion = incomingRevision
				var extra bytes.Buffer
				require.NoError(t, types.WriteInt32(&extra, H0))
				nullable := false
				extra.Write(types.EncodeBool(&nullable))
				require.NoError(t, types.WriteInt32(&extra, int32(len(sources))))
				for _, source := range sources {
					require.NoError(t, source.SaveIntermediateResultOfChunk(0, &extra))
				}
				partial.ExtraBuf = extra.Bytes()
				wire, err := partial.MarshalBinaryForPipeline(new(bytes.Buffer), true, true)
				require.NoError(t, err)
				decoded := batch.NewOffHeapEmpty()
				require.NoError(t, decoded.UnmarshalBinaryForPipeline(wire, proc.Mp()))
				_, err = merge.buildOneBatch(proc, decoded)
				decoded.Clean(proc.Mp())
				if incomingRevision != 0 {
					require.ErrorContains(t, err, "collation")
					require.False(t, merge.ctr.mergePartialMetadataSet)
					require.Empty(t, merge.ctr.groupByBatches, "no destination group may be published")
				} else {
					require.NoError(t, err, "a rejected partial must not poison a legacy retry")
					for _, ag := range merge.ctr.aggList {
						result := ag.PrepareParamKindVectorForChunk(0)
						require.Equal(t, "A", result.GetStringAt(0))
					}
				}
			}
		})
	}
}
