// Copyright 2021-2024 Matrix Origin
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
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightdedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestDupOperator(t *testing.T) {
	dupOperator(
		insert.NewPartitionInsert(
			&insert.Insert{},
			1,
		),
		0,
		0,
	)

	dupOperator(
		deletion.NewPartitionDelete(
			&deletion.Deletion{},
			1,
		),
		0,
		0,
	)

	assertFilter := filter.NewArgument()
	defer assertFilter.Release()
	assertFilter.IsAssert = true
	duplicatedFilter := dupOperator(assertFilter, 0, 1).(*filter.Filter)
	defer duplicatedFilter.Release()
	require.True(t, duplicatedFilter.IsAssert)
}

func TestConstructMergeGroupCarriesEmptyGroupingSetMetadata(t *testing.T) {
	groupNode := &plan.Node{GroupBy: []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
		{Typ: plan.Type{Id: int32(types.T_int32)}},
		{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
	}}
	expandNode := &plan.Node{
		GroupingFlag: []bool{true, true, true, false, false, false},
		ExtraOptions: "grouping_set_expand:3",
	}

	merge := constructMergeGroup(groupNode, expandNode, nil, true)
	defer merge.Release()
	require.Equal(t, []int64{2}, merge.EmptyGroupingSetIDs)
	require.Equal(t, []types.Type{
		types.NewWithCharset(types.T_varchar, 20, 0, 0),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
	}, merge.GroupByTypes)

	legacyGroupNode := &plan.Node{
		GroupBy:      groupNode.GroupBy[:2],
		GroupingFlag: []bool{false, false},
	}
	legacyMerge := constructMergeGroup(legacyGroupNode, &plan.Node{}, nil, true)
	defer legacyMerge.Release()
	require.True(t, legacyMerge.EmptyGroupingSet)
	require.Len(t, legacyMerge.GroupByTypes, 2)
}

func TestConstructRestrictForCheckConstraintNodes(t *testing.T) {
	assertExpr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "_check_constraint_assert"},
	}}}
	boolExpr := plan2.MakePlan2BoolConstExprWithType(true)
	for _, testCase := range []struct {
		node         *plan.Node
		wantFastPath bool
	}{
		{node: &plan.Node{NodeType: plan.Node_ASSERT, FilterList: []*plan.Expr{assertExpr}}, wantFastPath: true},
		{node: &plan.Node{NodeType: plan.Node_ASSERT, FilterList: []*plan.Expr{boolExpr}}},
		{node: &plan.Node{NodeType: plan.Node_FILTER, FilterList: []*plan.Expr{boolExpr}, FilterIsBarrier: true}},
	} {
		node := testCase.node
		op := constructRestrict(node, plan2.DeepCopyExprList(node.FilterList))
		require.Len(t, op.FilterExprs, len(node.FilterList))
		require.False(t, op.IsEnd,
			"CHECK operators must return their surviving batch to downstream DML")
		require.Equal(t, testCase.wantFastPath, op.IsAssert)
	}
}

func TestIdentityProjectionOfChild(t *testing.T) {
	identity := []*plan.Expr{
		{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
		{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
	}
	child := []*plan.Expr{{}, {}}
	require.True(t, isIdentityProjectionOfChild(identity, child))
	require.False(t, isIdentityProjectionOfChild(identity[:1], child))
	nonIdentity := plan2.DeepCopyExprList(identity)
	nonIdentity[1].GetCol().ColPos = 0
	require.False(t, isIdentityProjectionOfChild(nonIdentity, child))
}

func TestJoinHashBuildTopologyPinsSpillToSingleConsumer(t *testing.T) {
	operators := []vm.Operator{
		&hashjoin.HashJoin{EqConds: [][]*plan.Expr{{}, {}}},
		&dedupjoin.DedupJoin{Conditions: [][]*plan.Expr{{}, {}}},
		&rightdedupjoin.RightDedupJoin{Conditions: [][]*plan.Expr{{}, {}}},
	}

	for _, operator := range operators {
		t.Run(operator.OpType().String(), func(t *testing.T) {
			broadcast := constructBroadcastHashBuild(operator, nil, 4)
			require.False(t, broadcast.IsShuffle)
			require.Equal(t, int32(4), broadcast.JoinMapRefCnt)
			broadcast.Release()

			shuffle := constructShuffleHashBuild(&plan.Node{}, operator, nil)
			require.True(t, shuffle.IsShuffle)
			require.Equal(t, int32(1), shuffle.JoinMapRefCnt)
			shuffle.Release()
		})
	}
}

func TestLoopJoinBuildCarriesScalarRuntimeFilter(t *testing.T) {
	join := loopjoin.NewArgument()
	defer join.Release()
	join.JoinMapTag = 1
	spec := &plan.RuntimeFilterSpec{
		Tag: 42, UpperLimit: 1, ScalarPredicate: true,
	}

	op := constructJoinBuildOperator(
		&Compile{}, join, 1, []*plan.RuntimeFilterSpec{spec})
	build, ok := op.(*hashbuild.HashBuild)
	require.True(t, ok)
	defer build.Release()

	require.False(t, build.NeedHashMap)
	require.True(t, build.RuntimeFilterSpec.ScalarPredicate)
	require.Same(t, spec, build.RuntimeFilterSpec)
}

func TestConstructFuzzyFilterUsesFinalizedBuildSide(t *testing.T) {
	newNodes := func(side plan.Node_FuzzyBuildSide, tableCost, sinkCost float64) (
		*plan.Node, *plan.Node, *plan.Node, *plan.RuntimeFilterSpec,
	) {
		typ := plan.Type{Id: int32(types.T_int64)}
		spec := &plan.RuntimeFilterSpec{
			Tag:         1,
			BuildExpr:   &plan.Expr{Typ: typ},
			KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
		}
		node := &plan.Node{
			NodeType:       plan.Node_FUZZY_FILTER,
			FuzzyBuildSide: side,
			TableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "id", Typ: typ}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			},
			RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{spec},
		}
		tableScan := &plan.Node{
			NodeType: plan.Node_TABLE_SCAN,
			Stats:    &plan.Stats{Cost: tableCost},
			RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{
				Tag: 1, Expr: &plan.Expr{Typ: typ},
			}},
		}
		sinkScan := &plan.Node{
			NodeType: plan.Node_SINK_SCAN,
			Stats:    &plan.Stats{Cost: sinkCost},
		}
		return node, tableScan, sinkScan, spec
	}

	t.Run("sink decision survives rewritten cost ratio", func(t *testing.T) {
		node, tableScan, sinkScan, spec := newNodes(
			plan.Node_FUZZY_BUILD_SIDE_SINK, 8_192, 1_000_000)
		op := constructFuzzyFilter(node, tableScan, sinkScan)
		defer op.Release()

		require.Equal(t, 1, op.BuildIdx)
		require.Same(t, spec, op.RuntimeFilterSpec)
		require.Len(t, node.RuntimeFilterBuildList, 1)
		require.Len(t, tableScan.RuntimeFilterProbeList, 1)
	})

	t.Run("table decision overrides later cost drift", func(t *testing.T) {
		node, tableScan, sinkScan, _ := newNodes(
			plan.Node_FUZZY_BUILD_SIDE_TABLE, 1_000_000, 1)
		op := constructFuzzyFilter(node, tableScan, sinkScan)
		defer op.Release()

		require.Equal(t, 0, op.BuildIdx)
		require.Nil(t, op.RuntimeFilterSpec)
		require.Empty(t, node.RuntimeFilterBuildList)
		require.Empty(t, tableScan.RuntimeFilterProbeList)
	})

	t.Run("uses projected exact float identity type", func(t *testing.T) {
		node, tableScan, sinkScan, _ := newNodes(
			plan.Node_FUZZY_BUILD_SIDE_SINK, 10, 10)
		node.TableDef.Cols[0].Typ = plan.Type{Id: int32(types.T_float64)}
		identityType := plan.Type{Id: int32(types.T_varchar)}
		tableScan.ProjectList = []*plan.Expr{{Typ: identityType}}
		sinkScan.ProjectList = []*plan.Expr{{Typ: identityType}}

		op := constructFuzzyFilter(node, tableScan, sinkScan)
		defer op.Release()

		require.Equal(t, identityType, op.PkTyp)
	})

	t.Run("uses table projection when sink projection is absent", func(t *testing.T) {
		node, tableScan, sinkScan, _ := newNodes(
			plan.Node_FUZZY_BUILD_SIDE_TABLE, 10, 10)
		identityType := plan.Type{Id: int32(types.T_varchar)}
		tableScan.ProjectList = []*plan.Expr{{Typ: identityType}}

		op := constructFuzzyFilter(node, tableScan, sinkScan)
		defer op.Release()

		require.Equal(t, identityType, op.PkTyp)
	})
}

func TestConstructAggregateConfigIncludesGroupConcatMaxLen(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		require.Equal(t, "group_concat_max_len", name)
		require.True(t, system)
		require.False(t, global)
		return int64(5), nil
	})

	valueArg := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}}
	separatorArg := plan2.MakePlan2StringConstExprWithType("")
	args, config := constructAggregateConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NameGroupConcat},
		Args: []*plan.Expr{valueArg, separatorArg},
	}, proc)

	require.Equal(t, []*plan.Expr{valueArg}, args)
	require.Equal(t, aggexec.EncodeGroupConcatConfig("", 5), config)
}

func TestConstructAggregateConfigPreservesOrderedGroupConcatArgs(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		require.Equal(t, "group_concat_max_len", name)
		require.True(t, system)
		require.False(t, global)
		return int64(5), nil
	})

	valueArg := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}}
	orderArg := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}
	planConfig := []byte{1, 2, 3}
	args, config := constructAggregateConfig(&plan.Function{
		Func:          &plan.ObjectRef{ObjName: plan2.NameGroupConcat},
		Args:          []*plan.Expr{valueArg, orderArg},
		AggConfigType: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		AggConfig:     planConfig,
	}, proc)

	require.Equal(t, []*plan.Expr{valueArg, orderArg}, args)
	require.Equal(t, aggexec.EncodeGroupConcatOrderedConfig(planConfig, 5), config)
}

func TestConstructAggregateConfigApproxPercentileWithinGroup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	value := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}
	percentile := plan2.MakePlan2Float64ConstExprWithType(0.25)
	for _, tc := range []struct {
		name       string
		planConfig []byte
		want       string
	}{
		{name: "ordinary form", want: "0.25"},
		{name: "ordered ascending", planConfig: []byte{0}, want: "0.25"},
		{name: "ordered descending", planConfig: []byte{1}, want: "0.75"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args, config := constructAggregateConfig(&plan.Function{
				Func:      &plan.ObjectRef{ObjName: plan2.NameApproxPercentile},
				Args:      []*plan.Expr{value, percentile},
				AggConfig: tc.planConfig,
			}, proc)
			require.Equal(t, []*plan.Expr{value}, args)
			require.Equal(t, tc.want, string(config))
		})
	}
}

func TestComplementPercentileConfigPreservesDecimalScale(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{input: "0", want: "1"},
		{input: "1", want: "0"},
		{input: "0.95", want: "0.05"},
		{input: "0.500", want: "0.500"},
	} {
		actual, err := complementPercentileConfig([]byte(tc.input))
		require.NoError(t, err)
		require.Equal(t, tc.want, string(actual))
	}
	_, err := complementPercentileConfig([]byte("invalid"))
	require.Error(t, err)
}

func TestConstructAggregateConfigOrderedPercentile(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	value := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}
	percentile := plan2.MakePlan2Float64ConstExprWithType(0.95)
	args, config := constructAggregateConfig(&plan.Function{
		Func:      &plan.ObjectRef{ObjName: plan2.NamePercentileCont},
		Args:      []*plan.Expr{value, percentile},
		AggConfig: []byte{1},
	}, proc)

	require.Equal(t, []*plan.Expr{value}, args)
	require.Equal(t, aggexec.EncodeOrderedPercentileConfig([]byte("0.95"), true), config)

	args, config = constructAggregateConfig(&plan.Function{
		Func:      &plan.ObjectRef{ObjName: plan2.NamePercentileDisc},
		Args:      []*plan.Expr{value, plan2.MakePlan2Float64ConstExprWithType(0)},
		AggConfig: []byte{0},
	}, proc)
	require.Equal(t, []*plan.Expr{value}, args)
	require.Equal(t, aggexec.EncodeOrderedPercentileConfig([]byte("0"), false), config)
}

func TestConstructAggregateConfigOrderedPercentileRejectsInvalidInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	value := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}
	percentile := plan2.MakePlan2Float64ConstExprWithType(0.5)
	percentileColumn := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
	}

	for _, fn := range []string{plan2.NamePercentileCont, plan2.NamePercentileDisc} {
		t.Run(fn+" wrong argument count", func(t *testing.T) {
			require.Panics(t, func() {
				constructAggregateConfig(&plan.Function{
					Func: &plan.ObjectRef{ObjName: fn},
					Args: []*plan.Expr{value},
				}, proc)
			})
		})
		t.Run(fn+" nonconstant percentile", func(t *testing.T) {
			require.Panics(t, func() {
				constructAggregateConfig(&plan.Function{
					Func: &plan.ObjectRef{ObjName: fn},
					Args: []*plan.Expr{value, percentileColumn},
				}, proc)
			})
		})
	}

	args, config := constructAggregateConfig(&plan.Function{
		Func: &plan.ObjectRef{ObjName: plan2.NamePercentileCont},
		Args: []*plan.Expr{value, percentile},
	}, proc)
	require.Equal(t, []*plan.Expr{value}, args)
	require.Equal(t, aggexec.EncodeOrderedPercentileConfig([]byte("0.5"), false), config)
}

func TestDupHashBuildPreservesNullTracking(t *testing.T) {
	source := hashbuild.NewArgument()
	defer source.Release()
	source.TrackNullKeys = true

	duplicated := dupOperator(source, 0, 1).(*hashbuild.HashBuild)
	defer duplicated.Release()
	require.True(t, duplicated.TrackNullKeys)
}

func TestDupSetOperatorPreservesPhysicalEqualityKeys(t *testing.T) {
	source := intersectall.NewArgument()
	defer source.Release()
	source.KeyExprs = []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(7)}

	duplicated := dupOperator(source, 0, 1).(*intersectall.IntersectAll)
	defer duplicated.Release()
	require.Equal(t, source.KeyExprs, duplicated.KeyExprs)
}

func TestDupOperatorMergeTop(t *testing.T) {
	op := mergetop.NewArgument()
	op.Limit = plan2.MakePlan2Int64ConstExprWithType(10)
	op.Fs = []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}}
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for MergeTop")
	}
	dupOp := result.(*mergetop.MergeTop)
	if dupOp.Limit != op.Limit {
		t.Errorf("Limit mismatch")
	}
}

func TestDupOperatorMergeOrder(t *testing.T) {
	op := mergeorder.NewArgument()
	op.OrderBySpecs = []*plan.OrderBySpec{{Flag: plan.OrderBySpec_ASC}}
	op.SpillThreshold = 1234
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for MergeOrder")
	}
	dupOp := result.(*mergeorder.MergeOrder)
	if len(dupOp.OrderBySpecs) != len(op.OrderBySpecs) {
		t.Errorf("OrderBySpecs length mismatch: got %d, want %d", len(dupOp.OrderBySpecs), len(op.OrderBySpecs))
	}
	if dupOp.SpillThreshold != op.SpillThreshold {
		t.Errorf("SpillThreshold mismatch: got %d, want %d", dupOp.SpillThreshold, op.SpillThreshold)
	}
}

func TestDupOperatorOrderPreservesSpecsAndAllocationContract(t *testing.T) {
	op := order.NewArgument()
	defer op.Release()
	op.OrderBySpec = []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}}

	duplicated := dupOperator(op, 0, 1).(*order.Order)
	defer duplicated.Release()
	require.Equal(t, op.OrderBySpec, duplicated.OrderBySpec)
	_, ownsAllocation := any(duplicated).(executionAllocationAccountOwner)
	require.True(t, ownsAllocation)
}

func TestDupOperatorPartitionMultiUpdate(t *testing.T) {
	innerOp := multi_update.NewArgument()
	op := multi_update.NewPartitionMultiUpdate(innerOp)
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for PartitionMultiUpdate")
	}
}

func TestPartitionConstructionAndDuplicationPreserveHashConfiguration(t *testing.T) {
	node := &plan.Node{
		PartitionAlgorithm: plan.Node_PARTITION_ALGORITHM_HASH,
		SpillMem:           4096,
		OrderBy:            []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}},
	}
	op := constructPartition(node)
	require.Equal(t, node.PartitionAlgorithm, op.Algorithm)
	require.Equal(t, node.SpillMem, op.SpillMem)

	duplicated := dupOperator(op, 0, 1).(*partition.Partition)
	defer duplicated.Release()
	require.Equal(t, op.Algorithm, duplicated.Algorithm)
	require.Equal(t, op.SpillMem, duplicated.SpillMem)
}

func TestHasPartitionedUpdateTargetChecksEveryMainContext(t *testing.T) {
	contexts := []*plan.UpdateCtx{
		{TableDef: &plan.TableDef{TblId: 1}},
		{TableDef: &plan.TableDef{TblId: 2, FeatureFlag: features.Partitioned}},
		{TableDef: &plan.TableDef{TblId: 3, FeatureFlag: features.IndexTable}},
	}
	require.True(t, hasPartitionedUpdateTarget(contexts))

	contexts[1].TableDef.FeatureFlag = 0
	contexts[2].TableDef.FeatureFlag = features.Partitioned | features.IndexTable
	require.False(t, hasPartitionedUpdateTarget(contexts))
}

func TestDupOperatorMultiUpdateCountDeleteAffectRows(t *testing.T) {
	op := multi_update.NewArgument()
	op.Action = multi_update.UpdateWriteTable
	op.IsOnduplicateKeyUpdate = true
	op.CountDeleteAffectRows = true
	op.RejectZeroTemporal = true
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for MultiUpdate")
	}
	dupOp := result.(*multi_update.MultiUpdate)
	if !dupOp.CountDeleteAffectRows {
		t.Error("CountDeleteAffectRows not preserved by dupOperator")
	}
	if dupOp.Action != op.Action {
		t.Errorf("Action mismatch: got %v, want %v", dupOp.Action, op.Action)
	}
	if dupOp.IsOnduplicateKeyUpdate != op.IsOnduplicateKeyUpdate {
		t.Errorf("IsOnduplicateKeyUpdate mismatch: got %v, want %v",
			dupOp.IsOnduplicateKeyUpdate, op.IsOnduplicateKeyUpdate)
	}
	if !dupOp.RejectZeroTemporal {
		t.Error("RejectZeroTemporal not preserved by dupOperator")
	}
}

func TestDupOperatorPreInsertState(t *testing.T) {
	op := preinsert.NewArgument()
	op.RejectZeroTemporal = true
	op.HasTargetSelector = true
	op.TargetRowNumberCol = 7
	op.TargetActiveCol = 8
	op.TargetRowIDCol = 9
	result := dupOperator(op, 0, 1)
	require.NotNil(t, result)
	cloned := result.(*preinsert.PreInsert)
	require.True(t, cloned.RejectZeroTemporal)
	require.True(t, cloned.HasTargetSelector)
	require.Equal(t, int32(7), cloned.TargetRowNumberCol)
	require.Equal(t, int32(8), cloned.TargetActiveCol)
	require.Equal(t, int32(9), cloned.TargetRowIDCol)
}

func TestRefreshZeroTemporalWritePolicy(t *testing.T) {
	pre := preinsert.NewArgument()
	pre.RejectZeroTemporal = true
	multi := multi_update.NewArgument()
	multi.RejectZeroTemporal = true
	multi.AppendChild(pre)

	require.NoError(t, refreshZeroTemporalWritePolicy(multi, false))
	require.False(t, multi.RejectZeroTemporal)
	require.False(t, pre.RejectZeroTemporal)

	require.NoError(t, refreshZeroTemporalWritePolicy(multi, true))
	require.True(t, multi.RejectZeroTemporal)
	require.True(t, pre.RejectZeroTemporal)
}

func TestDupOperatorDispatchRecCTE(t *testing.T) {
	op := dispatch.NewArgument()
	op.RecCTE = true
	op.RecSink = true
	op.IsSink = true
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for Dispatch")
	}
	dupOp := result.(*dispatch.Dispatch)
	if dupOp.RecCTE != op.RecCTE {
		t.Errorf("RecCTE mismatch: got %v, want %v", dupOp.RecCTE, op.RecCTE)
	}
}

func TestConstructTimeWindowUsesRegularSumForCountCache(t *testing.T) {
	arg := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 1},
		},
	}
	ts := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 0},
		},
	}
	node := &plan.Node{
		AggList: []*plan.Expr{
			{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							Obj:     function.AggSumOverloadID,
							ObjName: "sum",
						},
						Args: []*plan.Expr{arg},
					},
				},
			},
		},
		GroupBy:   []*plan.Expr{ts},
		Timestamp: ts,
		Interval:  makeTimeWindowIntervalExpr(1, "second"),
	}

	timeWin := constructTimeWindow(context.Background(), node, nil)
	require.Len(t, timeWin.Aggs, 1)
	require.Equal(t, int64(function.AggSumOverloadID), timeWin.Aggs[0].GetAggID())
	require.Equal(t, types.T_int64, timeWin.Types[0].Oid)
}

func TestConstructTimeWindowUsesRegularSumForPartialSum(t *testing.T) {
	arg := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 0},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 1},
		},
	}
	ts := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 0},
		},
	}
	node := &plan.Node{
		AggList: []*plan.Expr{
			{
				Typ: plan.Type{Id: int32(types.T_uint64)},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							Obj:     function.AggSumOverloadID,
							ObjName: "sum",
						},
						Args: []*plan.Expr{arg},
					},
				},
			},
		},
		GroupBy:   []*plan.Expr{ts},
		Timestamp: ts,
		Interval:  makeTimeWindowIntervalExpr(1, "second"),
	}

	timeWin := constructTimeWindow(context.Background(), node, nil)
	require.Len(t, timeWin.Aggs, 1)
	require.Equal(t, int64(function.AggSumOverloadID), timeWin.Aggs[0].GetAggID())
	require.Equal(t, types.T_decimal128, timeWin.Types[0].Oid)
}

func TestConstructTimeWindowApproxPercentileConfig(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	fn, err := function.GetFunctionByName(context.Background(), plan2.NameApproxPercentile, []types.Type{
		types.T_int32.ToType(), types.T_float64.ToType(),
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		percentile *plan.Expr
		want       string
	}{
		{name: "lower endpoint", percentile: plan2.MakePlan2Float64ConstExprWithType(0), want: "0"},
		{name: "upper endpoint", percentile: plan2.MakePlan2Float64ConstExprWithType(1), want: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := makeTimeWindowAggNode(fn.GetEncodedOverloadID(), plan2.NameApproxPercentile, tc.percentile)
			arg := constructTimeWindow(context.Background(), node, proc)
			require.Len(t, arg.Aggs, 1)
			require.Len(t, arg.Aggs[0].GetArgExpressions(), 1)
			require.Equal(t, tc.want, string(arg.Aggs[0].GetExtraConfig()))
		})
	}
}

func TestConstructTimeWindowApproxPercentileRejectsInvalidConfig(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	fn, err := function.GetFunctionByName(context.Background(), plan2.NameApproxPercentile, []types.Type{
		types.T_int32.ToType(), types.T_float64.ToType(),
	})
	require.NoError(t, err)
	nonConstant := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2}},
	}

	for _, tc := range []struct {
		name       string
		percentile *plan.Expr
		want       string
	}{
		{
			name:       "below range",
			percentile: plan2.MakePlan2Float64ConstExprWithType(-0.01),
			want:       "invalid input: percentile argument of approx_percentile must be finite and in [0,1], got -0.01",
		},
		{
			name:       "above range",
			percentile: plan2.MakePlan2Float64ConstExprWithType(1.01),
			want:       "invalid input: percentile argument of approx_percentile must be finite and in [0,1], got 1.01",
		},
		{
			name:       "non constant",
			percentile: nonConstant,
			want:       "invalid input: percentile argument of approx_percentile must be a constant",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := makeTimeWindowAggNode(fn.GetEncodedOverloadID(), plan2.NameApproxPercentile, tc.percentile)
			require.PanicsWithError(t, tc.want, func() {
				constructTimeWindow(context.Background(), node, proc)
			})
		})
	}
}

func TestConstructAggregateConfigPreservesOtherSpecialConfigs(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		require.Equal(t, "group_concat_max_len", name)
		require.True(t, system)
		require.False(t, global)
		return int64(1024), nil
	})
	value := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
	}

	for _, tc := range []struct {
		name       string
		config     string
		wantConfig []byte
	}{
		{name: plan2.NameGroupConcat, config: "|", wantConfig: aggexec.EncodeGroupConcatConfig("|", 1024)},
		{name: plan2.NameClusterCenters, config: "k=3,init=random", wantConfig: []byte("k=3,init=random")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &plan.Function{
				Func: &plan.ObjectRef{ObjName: tc.name},
				Args: []*plan.Expr{value, plan2.MakePlan2StringConstExprWithType(tc.config)},
			}
			args, config := constructAggregateConfig(f, proc)
			require.Len(t, args, 1)
			require.Equal(t, tc.wantConfig, config)
		})
	}
}

func makeTimeWindowAggNode(functionID int64, name string, config *plan.Expr) *plan.Node {
	value := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
	}
	ts := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_datetime)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	return &plan.Node{
		AggList: []*plan.Expr{{
			Typ: plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: functionID, ObjName: name},
				Args: []*plan.Expr{value, config},
			}},
		}},
		GroupBy:   []*plan.Expr{ts},
		Timestamp: ts,
		Interval:  makeTimeWindowIntervalExpr(1, "second"),
	}
}

func TestConstructGapFillDisablesTumblingFastPath(t *testing.T) {
	gapFillStart := &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}}
	gapFillEnd := &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}}
	node := &plan.Node{
		NodeType:     plan.Node_TIME_WINDOW,
		Interval:     makeTimeWindowIntervalExpr(1, "minute"),
		GroupBy:      []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_datetime)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}},
		Timestamp:    &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}},
		WEnd:         &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		GapFillMode:  plan.Node_GAP_FILL_PARTITION,
		GapFillStart: gapFillStart,
		GapFillEnd:   gapFillEnd,
		ProjectList:  []*plan.Expr{},
		BindingTags:  []int32{},
		AggList:      []*plan.Expr{},
	}
	arg := constructTimeWindow(context.Background(), node, nil)
	require.True(t, arg.GapFill)
	require.Equal(t, arg.Interval, arg.Sliding)
	require.Same(t, gapFillStart, arg.GapFillStart)
	require.Same(t, gapFillEnd, arg.GapFillEnd)
	require.Nil(t, arg.EndExpr, "GAPFILL must not use the existing-window-only interval fast path")
	arg.Release()
}

func TestConstructTimeWindowPromotesDateBoundaryRuntimeType(t *testing.T) {
	dateTs := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_date)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 0},
		},
	}
	datetimeGroup := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 1, ColPos: 0},
		},
	}
	node := &plan.Node{
		NodeType:    plan.Node_TIME_WINDOW,
		Interval:    makeTimeWindowIntervalExpr(1, "minute"),
		GroupBy:     []*plan.Expr{datetimeGroup},
		Timestamp:   dateTs,
		WEnd:        datetimeGroup,
		ProjectList: []*plan.Expr{},
		BindingTags: []int32{},
		AggList: []*plan.Expr{
			{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{
						Obj:     function.AggSumOverloadID,
						ObjName: "sum",
					},
					Args: []*plan.Expr{{
						Typ: plan.Type{Id: int32(types.T_int64)},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{RelPos: 0, ColPos: 1},
						},
					}},
				}},
			},
			{
				Typ:  plan.Type{Id: int32(types.T_datetime), NotNullable: true},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: plan2.TimeWindowStart}},
			},
			{
				Typ:  plan.Type{Id: int32(types.T_datetime), NotNullable: true},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: plan2.TimeWindowEnd}},
			},
		},
	}

	arg := constructTimeWindow(context.Background(), node, nil)
	require.Equal(t, int32(types.T_datetime), arg.TsType.Id)
	require.True(t, arg.TsType.NotNullable)
	require.True(t, arg.WStart)
	require.True(t, arg.WEnd)
	arg.Release()
}

func TestProjectedMongoColumnsUsesExternalScanLayout(t *testing.T) {
	columns := []sqlmongodb.ColumnMapping{
		{Name: "id", Path: "_id"},
		{Name: "pump", Path: "pump"},
		{Name: "value", Path: "value"},
	}
	tableDef := &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "value"},
		{Name: "pump"},
		{Name: "__mo_hidden", Hidden: true},
	}}
	projected, err := projectedMongoColumns(t.Context(), columns, tableDef, false)
	require.NoError(t, err)
	require.Equal(t, []sqlmongodb.ColumnMapping{columns[2], columns[1]}, projected)

	_, err = projectedMongoColumns(t.Context(), columns, &plan.TableDef{Cols: []*plan.ColDef{{Name: "missing"}}}, false)
	require.Error(t, err)
	_, err = projectedMongoColumns(t.Context(), columns, nil, false)
	require.Error(t, err)
	queryOnly, err := projectedMongoColumns(t.Context(), columns, &plan.TableDef{Cols: []*plan.ColDef{{Name: "hidden", Hidden: true}}}, true)
	require.NoError(t, err)
	require.Empty(t, queryOnly)
	_, err = projectedMongoColumns(t.Context(), columns, &plan.TableDef{Cols: []*plan.ColDef{{Name: "hidden", Hidden: true}}}, false)
	require.Error(t, err)
}

func TestDupOperatorLoopJoinMarkPos(t *testing.T) {
	op := loopjoin.NewArgument()
	op.MarkPos = 3
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for LoopJoin")
	}
	dupOp := result.(*loopjoin.LoopJoin)
	if dupOp.MarkPos != op.MarkPos {
		t.Errorf("MarkPos mismatch: got %d, want %d", dupOp.MarkPos, op.MarkPos)
	}
}

func TestDupOperatorShuffleSharesPoolAcrossWorkers(t *testing.T) {
	op := shuffle.NewArgument()
	op.BucketNum = 4
	op.DrainAllBuckets = true
	op.StringHashKey = true

	dupCtx := newOperatorDupContext()
	dup1 := dupOperatorWithContext(op, 0, 2, dupCtx).(*shuffle.Shuffle)
	dup2 := dupOperatorWithContext(op, 1, 2, dupCtx).(*shuffle.Shuffle)

	require.Nil(t, op.GetShufflePool(), "duplicating must not mutate the reusable template")
	require.Same(t, dup1.GetShufflePool(), dup2.GetShufflePool())
	nextGeneration := dupOperatorWithContext(op, 0, 2, newOperatorDupContext()).(*shuffle.Shuffle)
	require.NotSame(t, dup1.GetShufflePool(), nextGeneration.GetShufflePool())
	require.Equal(t, int32(0), dup1.CurrentShuffleIdx)
	require.Equal(t, int32(1), dup2.CurrentShuffleIdx)
	require.True(t, dup1.DrainAllBuckets)
	require.True(t, dup2.DrainAllBuckets)
	require.True(t, dup1.StringHashKey)
	require.True(t, dup2.StringHashKey)
}

func TestDupOperatorDedupJoinSharesMailboxOnlyWithinGeneration(t *testing.T) {
	op := dedupjoin.NewArgument()

	dupCtx := newOperatorDupContext()
	dup1 := dupOperatorWithContext(op, 0, 2, dupCtx).(*dedupjoin.DedupJoin)
	dup2 := dupOperatorWithContext(op, 1, 2, dupCtx).(*dedupjoin.DedupJoin)

	require.Nil(t, op.Mailbox, "duplicating must not mutate the reusable template")
	require.Same(t, dup1.Mailbox, dup2.Mailbox)
	nextGeneration := dupOperatorWithContext(op, 0, 2, newOperatorDupContext()).(*dedupjoin.DedupJoin)
	require.NotSame(t, dup1.Mailbox, nextGeneration.Mailbox)
}

func TestDupOperatorHashJoinSharesMailboxOnlyWithinGeneration(t *testing.T) {
	op := hashjoin.NewArgument()
	op.EmitCompressedRowCount = true
	staleMailbox := hashjoin.NewBitmapMailbox(2)
	staleMailbox.SealAndDrain(mpool.MustNewZero())
	op.Mailbox = staleMailbox

	dupCtx := newOperatorDupContext()
	dup1 := dupOperatorWithContext(op, 0, 2, dupCtx).(*hashjoin.HashJoin)
	dup2 := dupOperatorWithContext(op, 1, 2, dupCtx).(*hashjoin.HashJoin)

	require.Same(t, staleMailbox, op.Mailbox, "duplicating must not mutate the reusable template")
	require.NotSame(t, staleMailbox, dup1.Mailbox, "a stale template mailbox must not enter a new execution")
	require.Same(t, dup1.Mailbox, dup2.Mailbox)
	require.True(t, dup1.EmitCompressedRowCount)
	require.True(t, dup2.EmitCompressedRowCount)
	nextGeneration := dupOperatorWithContext(op, 0, 2, newOperatorDupContext()).(*hashjoin.HashJoin)
	require.NotSame(t, dup1.Mailbox, nextGeneration.Mailbox)
}

func TestDupOperatorAssignsSharedShuffleConsumerIndex(t *testing.T) {
	hashBuild := hashbuild.NewArgument()
	hashBuild.IsShuffle = true
	hashBuild.ShuffleIdx = -1
	require.Equal(t, int32(2), dupOperator(hashBuild, 2, 4).(*hashbuild.HashBuild).ShuffleIdx)

	hashJoin := hashjoin.NewArgument()
	hashJoin.IsShuffle = true
	hashJoin.ShuffleIdx = -1
	require.Equal(t, int32(2), dupOperator(hashJoin, 2, 4).(*hashjoin.HashJoin).ShuffleIdx)

	dedupJoin := dedupjoin.NewArgument()
	dedupJoin.IsShuffle = true
	dedupJoin.ShuffleIdx = -1
	require.Equal(t, int32(2), dupOperator(dedupJoin, 2, 4).(*dedupjoin.DedupJoin).ShuffleIdx)

	rightDedupJoin := rightdedupjoin.NewArgument()
	rightDedupJoin.IsShuffle = true
	rightDedupJoin.ShuffleIdx = -1
	rightDedupJoin.InputKeysUnique = true
	require.Equal(t, int32(2), dupOperator(rightDedupJoin, 2, 4).(*rightdedupjoin.RightDedupJoin).ShuffleIdx)
	require.True(t, dupOperator(rightDedupJoin, 2, 4).(*rightdedupjoin.RightDedupJoin).InputKeysUnique)
}

func TestConstructShuffleOperatorForJoinSupportsColumnsAndExpressions(t *testing.T) {
	left := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 3}},
	}
	right := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "serial_full"},
		}},
	}
	node := &plan.Node{
		OnList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{left, right}}}}},
		Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
			ShuffleColIdx: 0,
			ShuffleType:   plan.ShuffleType_Range,
		}},
		RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{Tag: 42}},
	}

	leftShuffle := constructShuffleOperatorForJoin(4, node, true)
	require.Equal(t, int32(3), leftShuffle.ShuffleColIdx)
	require.Nil(t, leftShuffle.ShuffleExpr)
	require.Equal(t, int32(42), leftShuffle.RuntimeFilterSpec.Tag)

	rightShuffle := constructShuffleOperatorForJoin(4, node, false)
	require.Equal(t, right.Typ.Id, rightShuffle.ShuffleExpr.Typ.Id)
	require.Equal(t, "serial_full", rightShuffle.ShuffleExpr.GetF().Func.ObjName)
	require.Nil(t, rightShuffle.RuntimeFilterSpec)
}

func TestRangeShuffleJoinSingleBucketSkewedBatch(t *testing.T) {
	const (
		key       = int64(7)
		rowCount  = 8192
		bucketNum = int32(1)
	)
	left := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	right := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	node := &plan.Node{
		OnList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{left, right}}}}},
		Stats: &plan.Stats{
			TableCnt: rowCount,
			HashmapStats: &plan.HashMapStats{
				ShuffleColIdx: 0,
				ShuffleType:   plan.ShuffleType_Range,
				ShuffleColMin: key,
				ShuffleColMax: key,
				Ranges:        []float64{float64(key), float64(key), float64(key), float64(key)},
			},
		},
	}

	arg := constructShuffleOperatorForJoin(bucketNum, node, true)
	require.Nil(t, arg.ShuffleRangeInt64)

	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeScalarInt64(key, rowCount, mp)
	input.SetRowCount(rowCount)
	source := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(source)
	defer func() {
		source.Free(proc, false, nil)
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		arg.Release()
		proc.Free()
		require.Equal(t, int64(0), mp.CurrNB())
	}()
	require.NoError(t, vm.Prepare(arg, proc))

	rows := 0
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}
		require.Equal(t, int32(0), result.Batch.ShuffleIDX)
		rows += result.Batch.RowCount()
	}
	require.Equal(t, rowCount, rows)
}

func TestGetPercentileConfig(t *testing.T) {
	mp, err := mpool.NewMPool("test_pct_config", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("float64", func(t *testing.T) {
		vec, err := vector.NewConstFixed(types.T_float64.ToType(), float64(0.95), 1, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		cfg, err := getPercentileConfig(vec)
		require.NoError(t, err)
		require.Equal(t, "0.95", string(cfg))
	})

	t.Run("float32", func(t *testing.T) {
		vec, err := vector.NewConstFixed(types.T_float32.ToType(), float32(0.5), 1, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		cfg, err := getPercentileConfig(vec)
		require.NoError(t, err)
		require.Equal(t, "0.5", string(cfg))
	})

	t.Run("int64", func(t *testing.T) {
		vec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(0), 1, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		cfg, err := getPercentileConfig(vec)
		require.NoError(t, err)
		require.Equal(t, "0", string(cfg))
	})

	t.Run("int32", func(t *testing.T) {
		vec, err := vector.NewConstFixed(types.T_int32.ToType(), int32(1), 1, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		cfg, err := getPercentileConfig(vec)
		require.NoError(t, err)
		require.Equal(t, "1", string(cfg))
	})

	for _, tc := range []struct {
		name   string
		newVec func() (*vector.Vector, error)
		want   string
	}{
		{name: "bit", newVec: func() (*vector.Vector, error) {
			return vector.NewConstFixed(types.T_bit.ToType(), uint64(1), 1, mp)
		}, want: "1"},
		{name: "int8", newVec: func() (*vector.Vector, error) {
			return vector.NewConstFixed(types.T_int8.ToType(), int8(0), 1, mp)
		}, want: "0"},
		{name: "int16", newVec: func() (*vector.Vector, error) {
			return vector.NewConstFixed(types.T_int16.ToType(), int16(1), 1, mp)
		}, want: "1"},
		{name: "uint8", newVec: func() (*vector.Vector, error) {
			return vector.NewConstFixed(types.T_uint8.ToType(), uint8(1), 1, mp)
		}, want: "1"},
		{name: "uint16", newVec: func() (*vector.Vector, error) {
			return vector.NewConstFixed(types.T_uint16.ToType(), uint16(1), 1, mp)
		}, want: "1"},
		{name: "uint32", newVec: func() (*vector.Vector, error) {
			return vector.NewConstFixed(types.T_uint32.ToType(), uint32(1), 1, mp)
		}, want: "1"},
		{name: "uint64", newVec: func() (*vector.Vector, error) {
			return vector.NewConstFixed(types.T_uint64.ToType(), uint64(1), 1, mp)
		}, want: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec, err := tc.newVec()
			require.NoError(t, err)
			defer vec.Free(mp)
			cfg, err := getPercentileConfig(vec)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(cfg))
		})
	}

	t.Run("decimal64 preserves exact text", func(t *testing.T) {
		typ := types.New(types.T_decimal64, 10, 6)
		value, err := types.ParseDecimal64("0.123456", typ.Width, typ.Scale)
		require.NoError(t, err)
		vec, err := vector.NewConstFixed(typ, value, 1, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		cfg, err := getPercentileConfig(vec)
		require.NoError(t, err)
		require.Equal(t, "0.123456", string(cfg))
	})

	t.Run("decimal128 preserves exact text", func(t *testing.T) {
		typ := types.New(types.T_decimal128, 38, 30)
		value, err := types.ParseDecimal128("0.123456789012345678901234567890", typ.Width, typ.Scale)
		require.NoError(t, err)
		vec, err := vector.NewConstFixed(typ, value, 1, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		cfg, err := getPercentileConfig(vec)
		require.NoError(t, err)
		require.Equal(t, "0.123456789012345678901234567890", string(cfg))
	})
}

func TestGetPercentileConfigRejectsInvalidVectors(t *testing.T) {
	mp, err := mpool.NewMPool("test_pct_config_invalid", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	flat := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixed(flat, 0.5, false, mp))
	nullVec := vector.NewConstNull(types.T_float64.ToType(), 1, mp)
	unsupported, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("0.5"), 1, mp)
	require.NoError(t, err)
	below, err := vector.NewConstFixed(types.T_float64.ToType(), -0.1, 1, mp)
	require.NoError(t, err)
	above, err := vector.NewConstFixed(types.T_float64.ToType(), 1.1, 1, mp)
	require.NoError(t, err)
	nan, err := vector.NewConstFixed(types.T_float64.ToType(), math.NaN(), 1, mp)
	require.NoError(t, err)
	inf, err := vector.NewConstFixed(types.T_float64.ToType(), math.Inf(1), 1, mp)
	require.NoError(t, err)
	decimalType := types.New(types.T_decimal128, 38, 37)
	decimalAboveValue, err := types.ParseDecimal128(
		"1.0000000000000000000000000000000000001", decimalType.Width, decimalType.Scale)
	require.NoError(t, err)
	decimalAbove, err := vector.NewConstFixed(decimalType, decimalAboveValue, 1, mp)
	require.NoError(t, err)

	tests := []struct {
		name string
		vec  *vector.Vector
	}{
		{name: "non-constant", vec: flat},
		{name: "null", vec: nullVec},
		{name: "unsupported", vec: unsupported},
		{name: "below range", vec: below},
		{name: "above range", vec: above},
		{name: "nan", vec: nan},
		{name: "infinity", vec: inf},
		{name: "decimal above range beyond float64 precision", vec: decimalAbove},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.vec.Free(mp)
			require.NotPanics(t, func() {
				_, err := getPercentileConfig(tc.vec)
				require.Error(t, err)
			})
		})
	}
}

func TestValidateApproxPercentileExpr(t *testing.T) {
	column := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
	}
	parameter := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}

	require.Error(t, validateApproxPercentileExpr(nil))
	require.Error(t, validateApproxPercentileExpr(column))
	literal := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: 1},
		}},
	}
	require.NoError(t, validateApproxPercentileExpr(literal))
	require.Error(t, validateApproxPercentileExpr(parameter))
}

func TestValidateOrderedPercentileExpr(t *testing.T) {
	column := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
	}
	parameter := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	literal := plan2.MakePlan2Float64ConstExprWithType(0.5)

	require.Error(t, validateOrderedPercentileExpr(nil, plan2.NamePercentileCont))
	require.Error(t, validateOrderedPercentileExpr(column, plan2.NamePercentileCont))
	require.Error(t, validateOrderedPercentileExpr(parameter, plan2.NamePercentileDisc))
	require.NoError(t, validateOrderedPercentileExpr(literal, plan2.NamePercentileCont))
}

func makeTimeWindowIntervalExpr(value int64, unit string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_I64Val{I64Val: value},
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_Sval{Sval: unit},
							},
						},
					},
				},
			},
		},
	}
}

func TestDupOperatorTableFunctionPreservesProbeState(t *testing.T) {
	op := table_function.NewArgument()
	op.FuncName = "unnest"
	op.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
		{Tag: 8, UseMembershipFilter: true},
	}
	op.IndexReaderParam = &plan.IndexReaderParam{
		Limit:        plan2.MakePlan2Uint64ConstExprWithType(7),
		OrigFuncName: "l2_distance",
	}
	op.FulltextSourceRef = &plan.ObjectRef{SchemaName: "publisher", ObjName: "source", PubInfo: &plan.PubInfo{TenantId: 42}}
	op.FulltextIndexRef = &plan.ObjectRef{SchemaName: "publisher", ObjName: "index", PubInfo: &plan.PubInfo{TenantId: 42}}

	dup := dupOperator(op, 0, 1).(*table_function.TableFunction)
	require.Equal(t, op.RuntimeFilterSpecs, dup.RuntimeFilterSpecs)
	require.Equal(t, uint64(7), dup.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.Equal(t, "l2_distance", dup.IndexReaderParam.GetOrigFuncName())
	require.Equal(t, op.FulltextSourceRef, dup.FulltextSourceRef)
	require.Equal(t, op.FulltextIndexRef, dup.FulltextIndexRef)
}

func TestDupOperatorApplyPreservesFulltextReferences(t *testing.T) {
	tableFunction := table_function.NewArgument()
	tableFunction.FulltextSourceRef = &plan.ObjectRef{SchemaName: "publisher", ObjName: "source", PubInfo: &plan.PubInfo{TenantId: 42}}
	tableFunction.FulltextIndexRef = &plan.ObjectRef{SchemaName: "publisher", ObjName: "index", PubInfo: &plan.PubInfo{TenantId: 42}}
	op := &apply.Apply{TableFunction: tableFunction}

	dup := dupOperator(op, 0, 1).(*apply.Apply)
	require.Equal(t, tableFunction.FulltextSourceRef, dup.TableFunction.FulltextSourceRef)
	require.Equal(t, tableFunction.FulltextIndexRef, dup.TableFunction.FulltextIndexRef)
}
