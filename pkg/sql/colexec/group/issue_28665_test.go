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
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func issueOrderedGroupConcatAgg(separator string, maxLen uint64) aggexec.AggFuncExecExpression {
	config := []byte{2}
	config = binary.BigEndian.AppendUint32(config, 1)
	config = binary.BigEndian.AppendUint32(config, 1)
	config = append(config, 1)
	config = binary.BigEndian.AppendUint32(config, 1)
	config = binary.BigEndian.AppendUint32(config, uint32(len(separator)))
	config = append(config, separator...)
	agg := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfGroupConcat,
		false,
		[]*plan.Expr{colExpr(1, types.T_varchar), colExpr(2, types.T_int64)},
		config,
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
	)
	agg.SetExtraConfig(aggexec.EncodeGroupConcatOrderedConfig(config, maxLen))
	return agg
}

func TestGroupedGroupConcatWarningRowsUseInputOrdinalAcrossPartialMerge(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &groupConcatWarningSession{}
	proc.Session = session
	makeAgg := func() aggexec.AggFuncExecExpression {
		agg := orderedGroupConcatAgg(false)
		agg.SetExtraConfig(aggexec.EncodeGroupConcatOrderedConfig(agg.GetExtraConfig(), 4))
		return agg
	}

	makeInput := func(groups []int32, values []string, orderKeys []int64) *batch.Batch {
		input := batch.NewWithSize(3)
		input.Vecs[0] = testutil.MakeInt32Vector(groups, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeVarcharVector(values, nil, proc.Mp())
		input.Vecs[2] = testutil.MakeInt64Vector(orderKeys, nil, proc.Mp())
		input.SetRowCount(len(groups))
		return input
	}
	inputs := []*batch.Batch{
		makeInput([]int32{3, 3, 3}, []string{"p", "q", "r"}, []int64{1, 2, 3}),
		makeInput([]int32{1, 1, 1}, []string{"aa", "bbb", "cccc"}, []int64{1, 2, 3}),
		makeInput([]int32{2, 2, 2}, []string{"x", "yy", "zzz"}, []int64{1, 2, 3}),
	}
	child := colexec.NewMockOperator().WithBatchs(inputs)
	partial := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{makeAgg()})
	partial.NeedEval = false
	partial.AppendChild(child)
	require.NoError(t, partial.Prepare(proc))
	partials := collectBatches(t, partial, proc)
	require.Len(t, partials, 1)
	partialBatch := cloneBatch(t, proc, partials[0])
	partial.Free(proc, false, nil)
	child.Free(proc, false, nil)

	mergeChild := colexec.NewMockOperator().WithBatchs([]*batch.Batch{partialBatch})
	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{makeAgg()})
	merge.AppendChild(mergeChild)
	require.NoError(t, merge.Prepare(proc))
	outputs := collectBatches(t, merge, proc)
	require.Len(t, outputs, 1)
	values := make(map[int32]string, outputs[0].RowCount())
	groups := vector.MustFixedColNoTypeCheck[int32](outputs[0].Vecs[0])
	for i, group := range groups {
		values[group] = string(outputs[0].Vecs[1].GetBytesAt(i))
	}
	require.Equal(t, map[int32]string{1: "aa|b", 2: "x|yy", 3: "p|q|"}, values)
	require.Equal(t, []string{
		"Row 8 was cut by GROUP_CONCAT()",
		"Row 5 was cut by GROUP_CONCAT()",
		"Row 2 was cut by GROUP_CONCAT()",
	}, session.messages)
	merge.Free(proc, false, nil)
	mergeChild.Free(proc, false, nil)
	proc.Free()
}

func TestGroupedGroupConcatPartialProducersShareInputOrdinal(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &groupConcatWarningSession{}
	proc.Session = session
	makeAgg := func() aggexec.AggFuncExecExpression {
		agg := orderedGroupConcatAgg(false)
		agg.SetExtraConfig(aggexec.EncodeGroupConcatOrderedConfig(agg.GetExtraConfig(), 4))
		return agg
	}
	makePartial := func(value string, orderKey int64) *batch.Batch {
		input := batch.NewWithSize(3)
		input.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeVarcharVector([]string{value}, nil, proc.Mp())
		input.Vecs[2] = testutil.MakeInt64Vector([]int64{orderKey}, nil, proc.Mp())
		input.SetRowCount(1)
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		partial := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
			[]aggexec.AggFuncExecExpression{makeAgg()})
		partial.NeedEval = false
		partial.AppendChild(child)
		require.NoError(t, partial.Prepare(proc))
		outputs := collectBatches(t, partial, proc)
		require.Len(t, outputs, 1)
		result := cloneBatch(t, proc, outputs[0])
		partial.Free(proc, false, nil)
		child.Free(proc, false, nil)
		return result
	}
	partials := []*batch.Batch{
		makePartial("aa", 1),
		makePartial("bbb", 2),
	}
	mergeChild := colexec.NewMockOperator().WithBatchs(partials)
	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{makeAgg()})
	merge.AppendChild(mergeChild)
	t.Cleanup(func() {
		merge.Free(proc, false, nil)
		mergeChild.Free(proc, false, nil)
		proc.Free()
	})
	require.NoError(t, merge.Prepare(proc))
	outputs := collectBatches(t, merge, proc)
	require.Len(t, outputs, 1)
	require.Equal(t, "aa|b", string(outputs[0].Vecs[1].GetBytesAt(0)))
	require.Equal(t, []string{
		"Row 2 was cut by GROUP_CONCAT()",
	}, session.messages)
}

func TestGroupedGroupConcatWarningsPublishOnceAcrossSpillBuckets(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &groupConcatWarningSession{}
	proc.Session = session
	const rows = 512
	groups := make([]int32, rows)
	values := make([]string, rows)
	orderKeys := make([]int64, rows)
	for i := range rows {
		groups[i] = int32(i%3 + 1)
		values[i] = fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 256))
		orderKeys[i] = int64(i)
	}
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector(groups, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeVarcharVector(values, nil, proc.Mp())
	input.Vecs[2] = testutil.MakeInt64Vector(orderKeys, nil, proc.Mp())
	input.SetRowCount(rows)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	agg := orderedGroupConcatAgg(false)
	agg.SetExtraConfig(aggexec.EncodeGroupConcatOrderedConfig(agg.GetExtraConfig(), 4))
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{agg})
	g.SpillMem = 2
	g.AppendChild(child)
	t.Cleanup(func() {
		g.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
	})

	require.NoError(t, g.Prepare(proc))
	outputs := collectBatches(t, g, proc)
	require.NotEmpty(t, outputs)
	require.Equal(t, uint64(3), session.total)
	require.Equal(t, []string{
		"Row 3 was cut by GROUP_CONCAT()",
		"Row 2 was cut by GROUP_CONCAT()",
		"Row 1 was cut by GROUP_CONCAT()",
	}, session.messages)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadBuckets"])
}

func TestGroupedGroupConcatSpillPreservesMultiGroupBoundaryWarning(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &groupConcatWarningSession{}
	proc.Session = session
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 1, 2, 2, 2, 3, 3, 3}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeVarcharVector(
		[]string{"x", "yy", "zzz", "p", "q", "r", "aa", "bbb", "cccc"},
		nil, proc.Mp())
	input.Vecs[2] = testutil.MakeInt64Vector(
		[]int64{1, 2, 3, 1, 2, 3, 1, 2, 3}, nil, proc.Mp())
	input.SetRowCount(9)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	agg := issueOrderedGroupConcatAgg("|", 4)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{agg})
	g.SpillMem = 2
	g.AppendChild(child)
	t.Cleanup(func() {
		g.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
	})

	require.NoError(t, g.Prepare(proc))
	var outputs []*batch.Batch
	t.Cleanup(func() {
		for _, output := range outputs {
			output.Clean(proc.Mp())
		}
	})
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		outputs = append(outputs, cloneBatch(t, proc, result.Batch))
	}
	require.Len(t, outputs, 3)
	values := make(map[int32]string, 3)
	for _, output := range outputs {
		groups := vector.MustFixedColNoTypeCheck[int32](output.Vecs[0])
		for i, group := range groups {
			values[group] = string(output.Vecs[1].GetBytesAt(i))
		}
	}
	require.Equal(t, map[int32]string{
		1: "x|yy",
		2: "p|q|",
		3: "aa|b",
	}, values)
	// The three groups are deliberately spilled into separate singleton
	// buckets. Their logical grouped-query context must still select the
	// previous payload-contributing row at a zero-byte boundary.
	require.Equal(t, int64(3), g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadBuckets"])
	require.Equal(t, []string{
		"Row 8 was cut by GROUP_CONCAT()",
		"Row 5 was cut by GROUP_CONCAT()",
		"Row 2 was cut by GROUP_CONCAT()",
	}, session.messages)
}

func TestGroupedGroupConcatWarningUsesLastPayloadRowAfterEmptyValue(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &groupConcatWarningSession{}
	proc.Session = session
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 1, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeVarcharVector(
		[]string{"aaaa", "", "b", "z"}, nil, proc.Mp())
	input.Vecs[2] = testutil.MakeInt64Vector(
		[]int64{1, 2, 3, 1}, nil, proc.Mp())
	input.SetRowCount(4)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{issueOrderedGroupConcatAgg("", 4)})
	g.AppendChild(child)
	t.Cleanup(func() {
		g.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
	})

	require.NoError(t, g.Prepare(proc))
	outputs := collectBatches(t, g, proc)
	require.Len(t, outputs, 1)
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []string{"Row 1 was cut by GROUP_CONCAT()"}, session.messages)
}
