// Copyright 2026 Matrix Origin
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

package group

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Read each output before the next Call, which can release a reloaded spill bucket.
func variableGroupCounts(t *testing.T, proc *process.Process, op vm.Operator, columns int) map[string]int64 {
	t.Helper()
	got := make(map[string]int64)
	for {
		result, err := vm.Exec(op, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			return got
		}
		output := result.Batch
		require.Len(t, output.Vecs, columns+1)
		counts := vector.MustFixedColNoTypeCheck[int64](output.Vecs[columns])
		for row := 0; row < output.RowCount(); row++ {
			key := make([]any, columns)
			for column := range columns {
				if !output.Vecs[column].IsNull(uint64(row)) {
					key[column] = string(output.Vecs[column].GetBytesAt(row))
				}
			}
			encoded, err := json.Marshal(key)
			require.NoError(t, err)
			_, duplicate := got[string(encoded)]
			require.False(t, duplicate, "each complete key must have exactly one final group")
			got[string(encoded)] = counts[row]
		}
	}
}

func TestGroupVariableLengthKeys(t *testing.T) {
	for _, test := range []struct {
		name    string
		oid     types.T
		width   int32
		notNull bool
		rows    [][]string
		nullRow int
		want    map[string]int64
	}{
		{
			name: "nullable-varchar-prefix-duplicates-null-empty", oid: types.T_varchar, width: 3,
			rows: [][]string{{"多付"}, {"未付款"}, {"未付清"}, {"未付款"}, {""}, {""}}, nullRow: 5,
			want: map[string]int64{`["多付"]`: 1, `["未付款"]`: 2, `["未付清"]`: 1, `[""]`: 1, `[null]`: 1},
		},
		{
			name: "not-null-varchar-control", oid: types.T_varchar, width: 3, notNull: true,
			rows: [][]string{{"多付"}, {"未付款"}, {"未付清"}}, nullRow: -1,
			want: map[string]int64{`["多付"]`: 1, `["未付款"]`: 1, `["未付清"]`: 1},
		},
		{
			name: "not-null-varchar-differs-after-eight-bytes", oid: types.T_varchar, width: 3, notNull: true,
			rows: [][]string{{"未付一"}, {"未付丁"}, {"未付一"}}, nullRow: -1,
			want: map[string]int64{`["未付一"]`: 2, `["未付丁"]`: 1},
		},
		{
			name: "nullable-char-prefix", oid: types.T_char, width: 3,
			rows: [][]string{{"未付款"}, {"未付清"}}, nullRow: -1,
			want: map[string]int64{`["未付款"]`: 1, `["未付清"]`: 1},
		},
		{
			name: "not-null-varchar-field-boundaries", oid: types.T_varchar, width: 2, notNull: true,
			rows: [][]string{{"a", "bc"}, {"ab", "c"}, {"a", "bc"}}, nullRow: -1,
			want: map[string]int64{`["a","bc"]`: 2, `["ab","c"]`: 1},
		},
		{
			name: "not-null-varbinary-field-boundaries", oid: types.T_varbinary, width: 2, notNull: true,
			rows: [][]string{{"a", "bc"}, {"ab", "c"}}, nullRow: -1,
			want: map[string]int64{`["a","bc"]`: 1, `["ab","c"]`: 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			columns := len(test.rows[0])
			groupBy := make([]*plan.Expr, columns)
			for column := range columns {
				groupBy[column] = colExpr(int32(column), test.oid)
				groupBy[column].Typ.Width = test.width
				groupBy[column].Typ.NotNullable = test.notNull
			}
			aggs := []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar, false, groupBy[:1], nil)}
			g := newGroupOp(proc, groupBy, aggs)
			t.Cleanup(func() { g.Free(proc, false, nil) })
			child := colexec.NewMockOperator()
			t.Cleanup(func() { child.Free(proc, false, nil) })
			g.AppendChild(child)
			for execution := 0; execution < 2; execution++ {
				input := batch.NewWithSize(columns)
				child.WithBatchs([]*batch.Batch{input})
				for column := range columns {
					input.Vecs[column] = vector.NewVec(types.New(test.oid, test.width, 0))
					for row, values := range test.rows {
						require.NoError(t, vector.AppendBytes(input.Vecs[column], []byte(values[column]),
							row == test.nullRow, proc.Mp()))
					}
				}
				input.SetRowCount(len(test.rows))
				require.NoError(t, g.Prepare(proc))
				// Assert the public operator result before its codec, so the old
				// implementation fails for lost groups, not merely a changed layout.
				require.Equal(t, test.want, variableGroupCounts(t, proc, g, columns))
				require.Equal(t, int32(HStr), g.ctr.mtyp)
				g.Reset(proc, false, nil)
				child.Free(proc, false, nil)
				require.Zero(t, proc.Mp().CurrNB())
			}
		})
	}
}

func TestGroupVariableLengthKeysSpillAndMerge(t *testing.T) {
	for _, partialMerge := range []bool{false, true} {
		name := "group-spill"
		if partialMerge {
			name = "partial-group-merge-spill"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			key := colExpr(0, types.T_varchar)
			key.Typ.Width = 3
			aggs := []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar, false, []*plan.Expr{key}, nil)}
			g := newGroupOp(proc, []*plan.Expr{key}, aggs)
			g.NeedEval = !partialMerge
			// Below 10K this is a deterministic group-count threshold, not bytes.
			g.SpillMem = 1
			t.Cleanup(func() { g.Free(proc, false, nil) })
			child := colexec.NewMockOperator()
			t.Cleanup(func() { child.Free(proc, false, nil) })
			g.AppendChild(child)
			var merge *MergeGroup
			var output vm.Operator = g
			if partialMerge {
				merge = newMergeGroupOp(aggs)
				merge.SpillMem = 1
				merge.AppendChild(g)
				t.Cleanup(func() { merge.Free(proc, false, nil) })
				output = merge
			}
			for execution := 0; execution < 2; execution++ {
				for _, values := range [][]string{{"多付", "未付款", "未付清"}, {"未付清", "未付款", "未付款"}} {
					input := batch.NewWithSize(1)
					child.WithBatchs([]*batch.Batch{input})
					input.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 3, 0))
					for _, value := range values {
						require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(value), false, proc.Mp()))
					}
					input.SetRowCount(len(values))
				}
				require.NoError(t, g.Prepare(proc))
				if merge != nil {
					require.NoError(t, merge.Prepare(proc))
				}
				require.Equal(t, map[string]int64{`["多付"]`: 1, `["未付款"]`: 3, `["未付清"]`: 2},
					variableGroupCounts(t, proc, output, 1))
				require.Equal(t, int32(HStr), g.ctr.mtyp)
				if merge != nil {
					require.Equal(t, int32(HStr), merge.ctr.mtyp)
					require.Positive(t, merge.OpAnalyzer.GetOpStats().SpillRows)
					merge.Reset(proc, false, nil)
				} else {
					require.Positive(t, g.OpAnalyzer.GetOpStats().SpillRows)
				}
				g.Reset(proc, false, nil)
				child.Free(proc, false, nil)
				require.Zero(t, proc.Mp().CurrNB())
			}
		})
	}
}

func TestGroupFixedKeyCodecBoundary(t *testing.T) {
	for _, notNull := range []bool{false, true} {
		proc := testutil.NewProcess(t)
		key := colExpr(0, types.T_int64)
		key.Typ.NotNullable = notNull
		g := newGroupOp(proc, []*plan.Expr{key}, nil)
		func() {
			defer proc.Free()
			defer g.Free(proc, false, nil)
			require.NoError(t, g.Prepare(proc))
			want := int32(HStr)
			if notNull {
				want = H8
			}
			require.Equal(t, want, g.ctr.mtyp)
		}()
		require.Zero(t, proc.Mp().CurrNB())
	}
}
