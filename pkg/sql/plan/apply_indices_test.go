// Copyright 2024 Matrix Origin
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

package plan

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexOnlyScanGuard_RandomRangesScenario(t *testing.T) {
	// Simulates sysbench random_ranges on a 10M-row table with secondary index on k.
	// Query: SELECT count(k) FROM sbtest1 WHERE k BETWEEN ? AND ? OR k BETWEEN ? AND ? ...
	// With 10 ranges, estimated outcnt ≈ 10000, selectivity ≈ 0.001.
	//
	// The old guard used InFilterCardLimitNonPK (10000) which rejects this case.
	// The new guard uses GetInFilterCardLimitOnPK which for a 10M table returns 1000000.
	tableCnt := float64(10_000_000)
	outcnt := float64(10_000)
	selectivity := 0.001

	// Old behavior (regression): would reject index-only scan
	oldThreshold := float64(InFilterCardLimitNonPK) // 10000
	oldReject := selectivity >= InFilterSelectivityLimit || outcnt >= oldThreshold
	assert.True(t, oldReject, "old guard should reject random_ranges (outcnt=10000 >= threshold=10000)")

	// New behavior (fix): uses PK card limit scaled to table size
	// GetInFilterCardLimitOnPK("", 10M) = min(10M*0.3, 1M) = 1M (capped at InFilterCardLimitPK)
	newThreshold := float64(GetInFilterCardLimitOnPK("", tableCnt))
	newReject := selectivity >= InFilterSelectivityLimit || outcnt >= newThreshold
	assert.False(t, newReject, "new guard should allow random_ranges (outcnt=10000 < threshold=%v)", newThreshold)
	assert.Equal(t, int32(1_000_000), GetInFilterCardLimitOnPK("", tableCnt))

	// The OOM scenario: truly non-selective query on 10M table (selectivity 0.5, outcnt 5M)
	oomOutcnt := float64(5_000_000)
	oomSelectivity := 0.5
	oomRejectOld := oomSelectivity >= InFilterSelectivityLimit || oomOutcnt >= oldThreshold
	oomRejectNew := oomSelectivity >= InFilterSelectivityLimit || oomOutcnt >= newThreshold
	assert.True(t, oomRejectOld, "old guard should reject non-selective scan")
	assert.True(t, oomRejectNew, "new guard should also reject non-selective scan (selectivity >= 0.3)")
}

func TestIndexHintMissingIndexReturnsMysqlKeyDoesNotExist(t *testing.T) {
	mock := NewMockOptimizer(true)
	_, err := runOneStmt(mock, t, "select val from single_idx_t force index(idx_missing) where val = 1")
	require.Error(t, err)

	var moErr *moerr.Error
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, moerr.ER_KEY_DOES_NOT_EXIST, moErr.MySQLCode())
	require.Equal(t, "42000", moErr.SqlState())
	require.Contains(t, moErr.Error(), "Key 'idx_missing' doesn't exist in table 'single_idx_t'")
}

func TestIndexVisibilityMetadataDoesNotRejectHint(t *testing.T) {
	mock := NewMockOptimizer(true)
	indexDef := mock.ctxt.tables["single_idx_t"].Indexes[0]
	indexDef.Visible = false

	_, err := runOneStmt(mock, t, "select val from single_idx_t force index(idx_val) where val = 1")
	require.NoError(t, err)
}

func TestSingleColumnUniqueDecimalRangeUsesIndex(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	decimalType := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	mainTable := mock.ctxt.tables["index_hint_t"]
	mainTable.Cols[1].Typ = decimalType
	mainTable.Indexes = []*planpb.IndexDef{
		{
			IndexName:      "uk_a",
			Parts:          []string{"a"},
			IndexTableName: "uk_hint_a",
			TableExist:     true,
			Unique:         true,
		},
	}
	addIndexHintIndexTableForTest(mock, "uk_hint_a", 25365)
	mock.ctxt.tables["uk_hint_a"].Cols[0].Typ = decimalType

	queryPlan, err := runOneStmt(mock, t, `
		select b
		from index_hint_t force index(uk_a)
		where a > 10.255000`)
	require.NoError(t, err)
	require.True(t, planHasIndexJoin(queryPlan))
	require.Equal(t, "uk_a", findFirstIndexScanName(queryPlan))
}

func TestDirectUniqueDecimalRangeResidualFilterUsesDirectKey(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	decimalType := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	mainTable := mock.ctxt.tables["index_hint_t"]
	mainTable.Cols[1].Typ = decimalType
	mainTable.Indexes = []*planpb.IndexDef{
		{
			IndexName:      "uk_a",
			Parts:          []string{"a"},
			IndexTableName: "uk_hint_a",
			TableExist:     true,
			Unique:         true,
		},
		{
			IndexName:      "idx_a",
			Parts:          []string{"a", catalog.CreateAlias("id")},
			IndexTableName: "idx_hint_a",
			TableExist:     true,
		},
	}
	addIndexHintIndexTableForTest(mock, "uk_hint_a", 25365)
	mock.ctxt.tables["uk_hint_a"].Cols[0].Typ = decimalType
	addIndexHintIndexTableForTest(mock, "idx_hint_a", 25366)

	queryPlan, err := runOneStmt(mock, t, `
		select b
		from index_hint_t force index(uk_a, idx_a)
		where a >= 10.255000 and a >= 20.255000`)
	require.NoError(t, err)
	require.True(t, planHasIndexJoin(queryPlan))
	indexScan := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, indexScan)
	require.Equal(t, "uk_a", indexScan.IndexScanInfo.IndexName)
	require.Len(t, indexScan.FilterList, 2)

	residualFn := indexScan.FilterList[1].GetF()
	require.NotNil(t, residualFn)
	require.Len(t, residualFn.Args, 2)
	for _, arg := range residualFn.Args {
		require.NotNil(t, arg)
	}
	residualColExpr := residualFn.Args[0]
	if residualColExpr.GetCol() == nil {
		residualColExpr = residualFn.Args[1]
	}
	residualCol := residualColExpr.GetCol()
	require.NotNil(t, residualCol)
	require.Equal(t, int32(0), residualCol.ColPos)
	require.Nil(t, residualColExpr.GetF())
	require.Equal(t, decimalType, residualColExpr.Typ)
}

func TestEncodedRegularIndexCostRejectsRoundingDecimalRange(t *testing.T) {
	makeOptimizer := func() *MockOptimizer {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)
		mock.ctxt.tables["index_hint_t"].Cols[1].Typ = planpb.Type{
			Id: int32(types.T_decimal64), Width: 10, Scale: 2,
		}
		return mock
	}

	t.Run("rounding bounds fall back to the base scan", func(t *testing.T) {
		queryPlan, err := runOneStmt(makeOptimizer(), t, `
			select b
			from index_hint_t force index(idx_a)
			where a > 10.255000 and a <= 15.755000`)
		require.NoError(t, err)
		require.Empty(t, findFirstIndexScanName(queryPlan))
		require.False(t, planHasIndexJoin(queryPlan))
	})

	t.Run("lossless bounds retain the forced serialized index", func(t *testing.T) {
		queryPlan, err := runOneStmt(makeOptimizer(), t, `
			select b
			from index_hint_t force index(idx_a)
			where a > 10.250000 and a <= 15.750000`)
		require.NoError(t, err)
		require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))
		require.True(t, planHasIndexJoin(queryPlan))
	})
}

func TestApplyExtraFiltersOnIndexUsesPhysicalKeyEncoding(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int64)}
	varcharType := planpb.Type{Id: int32(types.T_varchar)}
	makeIndexNode := func(bindingTag int32, keyType, primaryType planpb.Type) *planpb.Node {
		return &planpb.Node{
			BindingTags: []int32{bindingTag},
			TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: keyType},
				{Name: catalog.IndexTablePrimaryColName, Typ: primaryType},
			}},
		}
	}

	t.Run("serialized index part", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		baseTag := builder.genNewBindTag()
		indexTag := builder.genNewBindTag()
		filter := makeTypedInt64RangeFilterExpr(baseTag, 1, ">=", 10, intType)
		node := &planpb.Node{
			TableDef: &planpb.TableDef{
				Cols: []*planpb.ColDef{
					{Name: "id", Typ: intType},
					{Name: "a", Typ: intType},
				},
				Name2ColIndex: map[string]int32{"id": 0, "a": 1},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
			BindingTags: []int32{baseTag},
			FilterList:  []*planpb.Expr{filter},
		}
		idxDef := &planpb.IndexDef{Parts: []string{"a", catalog.CreateAlias("id")}}
		indexNode := makeIndexNode(indexTag, varcharType, intType)

		builder.applyExtraFiltersOnIndex(idxDef, node, indexNode, nil)

		require.Len(t, indexNode.FilterList, 1)
		mapped := indexNode.FilterList[0].GetF().Args[0]
		require.Equal(t, "serial_extract", wrappedSerialFuncName(t, mapped))
		require.Equal(t, int32(0), mapped.GetF().Args[0].GetCol().ColPos)
	})

	t.Run("all serialized residuals", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		baseTag := builder.genNewBindTag()
		indexTag := builder.genNewBindTag()
		node := &planpb.Node{
			TableDef: &planpb.TableDef{
				Cols: []*planpb.ColDef{
					{Name: "id", Typ: intType},
					{Name: "a", Typ: intType},
				},
				Name2ColIndex: map[string]int32{"id": 0, "a": 1},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
			BindingTags: []int32{baseTag},
			FilterList: []*planpb.Expr{
				makeTypedInt64RangeFilterExpr(baseTag, 1, ">=", 10, intType),
				makeTypedInt64RangeFilterExpr(baseTag, 1, "<", 20, intType),
			},
		}
		idxDef := &planpb.IndexDef{Parts: []string{"a", catalog.CreateAlias("id")}}
		indexNode := makeIndexNode(indexTag, varcharType, intType)

		builder.applyExtraFiltersOnIndex(idxDef, node, indexNode, nil)

		require.Len(t, indexNode.FilterList, 2)
		for _, filter := range indexNode.FilterList {
			require.Equal(t, "serial_extract", wrappedSerialFuncName(t, filter.GetF().Args[0]))
		}
	})

	t.Run("composite primary key part", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		baseTag := builder.genNewBindTag()
		indexTag := builder.genNewBindTag()
		filter := makeTypedInt64RangeFilterExpr(baseTag, 0, ">=", 10, intType)
		filter.GetF().Args[0].GetCol().Name = "tenant_id"
		node := &planpb.Node{
			TableDef: &planpb.TableDef{
				Cols: []*planpb.ColDef{
					{Name: "tenant_id", Typ: intType},
					{Name: "id", Typ: intType},
					{Name: "a", Typ: intType},
					{Name: catalog.CPrimaryKeyColName, Typ: varcharType},
				},
				Name2ColIndex: map[string]int32{
					"tenant_id":                0,
					"id":                       1,
					"a":                        2,
					catalog.CPrimaryKeyColName: 3,
				},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: catalog.CPrimaryKeyColName,
					Names:       []string{"tenant_id", "id"},
				},
			},
			BindingTags: []int32{baseTag},
			FilterList:  []*planpb.Expr{filter},
		}
		idxDef := &planpb.IndexDef{Parts: []string{"a", catalog.CreateAlias(catalog.CPrimaryKeyColName)}}
		indexNode := makeIndexNode(indexTag, varcharType, varcharType)

		builder.applyExtraFiltersOnIndex(idxDef, node, indexNode, nil)

		require.Len(t, indexNode.FilterList, 1)
		mapped := indexNode.FilterList[0].GetF().Args[0]
		require.Equal(t, "serial_extract", wrappedSerialFuncName(t, mapped))
		require.Equal(t, int32(1), mapped.GetF().Args[0].GetCol().ColPos)

		invalidPrimaryNode := makeIndexNode(builder.genNewBindTag(), varcharType, intType)
		builder.applyExtraFiltersOnIndex(idxDef, node, invalidPrimaryNode, nil)
		require.Empty(t, invalidPrimaryNode.FilterList)
	})

	t.Run("invalid serialized key metadata skips optional pushdown", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		baseTag := builder.genNewBindTag()
		indexTag := builder.genNewBindTag()
		node := &planpb.Node{
			TableDef: &planpb.TableDef{
				Cols:          []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "a", Typ: intType}},
				Name2ColIndex: map[string]int32{"id": 0, "a": 1},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
			BindingTags: []int32{baseTag},
			FilterList:  []*planpb.Expr{makeTypedInt64RangeFilterExpr(baseTag, 1, ">=", 10, intType)},
		}
		idxDef := &planpb.IndexDef{Parts: []string{"a", catalog.CreateAlias("id")}}
		indexNode := makeIndexNode(indexTag, intType, intType)

		builder.applyExtraFiltersOnIndex(idxDef, node, indexNode, nil)

		require.Empty(t, indexNode.FilterList)
	})
}

func TestResolveRegularIndexBackfillResidualAccess(t *testing.T) {
	simpleTable := &planpb.TableDef{
		Name2ColIndex: map[string]int32{"id": 0, "status": 1, "note": 2},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
	compoundTable := &planpb.TableDef{
		Name2ColIndex: map[string]int32{
			"tenant_id": 0,
			"event_id":  1,
			"status":    2,
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Names:       []string{"tenant_id", "event_id"},
		},
	}

	tests := []struct {
		name          string
		idxDef        *planpb.IndexDef
		tableDef      *planpb.TableDef
		colPos        int32
		prefixLengths map[string]int
		wantSource    regularIndexBackfillResidualSource
		wantPosition  int
	}{
		{
			name:       "direct unique index key",
			idxDef:     &planpb.IndexDef{Parts: []string{"status"}, Unique: true},
			tableDef:   simpleTable,
			colPos:     1,
			wantSource: regularIndexResidualIndexKey,
		},
		{
			name:          "lossy prefix index key",
			idxDef:        &planpb.IndexDef{Parts: []string{"status"}, Unique: true},
			tableDef:      simpleTable,
			colPos:        1,
			prefixLengths: map[string]int{"status": 4},
			wantSource:    regularIndexResidualUnavailable,
		},
		{
			name:         "separately stored simple primary key",
			idxDef:       &planpb.IndexDef{Parts: []string{"status"}, Unique: true},
			tableDef:     simpleTable,
			colPos:       0,
			wantSource:   regularIndexResidualPhysicalPK,
			wantPosition: 0,
		},
		{
			name:         "simple primary key appended to serialized index key",
			idxDef:       &planpb.IndexDef{Parts: []string{"status", catalog.CreateAlias("id")}},
			tableDef:     simpleTable,
			colPos:       0,
			wantSource:   regularIndexResidualIndexKey,
			wantPosition: 1,
		},
		{
			name:         "compound primary key component",
			idxDef:       &planpb.IndexDef{Parts: []string{"status"}, Unique: true},
			tableDef:     compoundTable,
			colPos:       1,
			wantSource:   regularIndexResidualCompoundPK,
			wantPosition: 1,
		},
		{
			name:       "unavailable non-key column",
			idxDef:     &planpb.IndexDef{Parts: []string{"status"}, Unique: true},
			tableDef:   simpleTable,
			colPos:     2,
			wantSource: regularIndexResidualUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveRegularIndexBackfillResidualAccess(
				tt.idxDef, tt.tableDef, tt.colPos, tt.prefixLengths, nil,
			)
			require.Equal(t, tt.wantSource, got.source)
			require.Equal(t, tt.wantPosition, got.position)

			knownPartPositions := []int{-1, -1, -1}
			for partPos, part := range tt.idxDef.Parts {
				if colPos, ok := tt.tableDef.Name2ColIndex[catalog.ResolveAlias(part)]; ok {
					knownPartPositions[colPos] = partPos
				}
			}
			fastPath := resolveRegularIndexBackfillResidualAccess(
				tt.idxDef, tt.tableDef, tt.colPos, tt.prefixLengths, knownPartPositions,
			)
			require.Equal(t, got, fastPath,
				"the scorer's precomputed part map must preserve materializer resolution")
		})
	}
}

func TestFilterRegularIndexesByScanHints(t *testing.T) {
	idxA := &planpb.IndexDef{IndexName: "idx_a"}
	idxB := &planpb.IndexDef{IndexName: "idx_b"}
	indexes := []*planpb.IndexDef{idxA, idxB}
	node := &planpb.Node{NodeId: 7}

	testCases := []struct {
		name string
		hint indexHintScopeSet
		want []string
	}{
		{
			name: "use",
			hint: indexHintScopeSet{useSpecified: true, use: map[string]struct{}{"idx_b": {}}},
			want: []string{"idx_b"},
		},
		{
			name: "force",
			hint: indexHintScopeSet{forceSpecified: true, force: map[string]struct{}{"idx_a": {}}},
			want: []string{"idx_a"},
		},
		{
			name: "empty use disables indexes",
			hint: indexHintScopeSet{useSpecified: true},
			want: []string{},
		},
		{
			name: "ignore",
			hint: indexHintScopeSet{ignore: map[string]struct{}{"idx_a": {}}},
			want: []string{"idx_b"},
		},
		{
			name: "ignore wins over force",
			hint: indexHintScopeSet{
				forceSpecified: true,
				force:          map[string]struct{}{"idx_a": {}},
				ignore:         map[string]struct{}{"idx_a": {}},
			},
			want: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := &QueryBuilder{
				indexHintsByScan: map[int32]*indexHintSet{
					node.NodeId: {scan: tc.hint},
				},
			}
			got := builder.filterRegularIndexesByScanHints(node, indexes)
			gotNames := make([]string, 0, len(got))
			for _, idx := range got {
				gotNames = append(gotNames, idx.IndexName)
			}
			require.Equal(t, tc.want, gotNames)
		})
	}
}

func TestRecordIndexHintsValidatesNames(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	tableDef := &planpb.TableDef{
		Name: "t",
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
		Indexes: []*planpb.IndexDef{
			{IndexName: "idx_a", TableExist: true},
			{IndexName: "idx_unavailable"},
		},
	}

	err := builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
		{HintType: tree.HintForce, HintScope: tree.HintForScan, IndexNames: []string{"PRIMARY"}},
	})
	require.NoError(t, err)

	err = builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
		{HintType: tree.HintUse, HintScope: tree.HintForScan, IndexNames: []string{"idx_missing"}},
	})
	require.Error(t, err)

	var moErr *moerr.Error
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, moerr.ER_KEY_DOES_NOT_EXIST, moErr.MySQLCode())

	err = builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
		{HintType: tree.HintForce, HintScope: tree.HintForScan, IndexNames: []string{"idx_unavailable"}},
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, moerr.ER_KEY_DOES_NOT_EXIST, moErr.MySQLCode())
}

func TestIndexHintNamesUseCanonicalIdentifierComparison(t *testing.T) {
	tableDef := &planpb.TableDef{
		Name: "t",
		Indexes: []*planpb.IndexDef{
			{IndexName: "Σ", TableExist: true},
			{IndexName: "ς", TableExist: true},
		},
	}

	names, err := validateIndexHintNames(context.Background(), tableDef, []string{"ς", "σ"})
	require.NoError(t, err)
	require.Equal(t, []string{indexNameKey("ς"), indexNameKey("Σ")}, names)
	require.NotEqual(t, names[0], names[1])
}

func TestIndexAccessUsesCanonicalIdentifierComparison(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{
			NodeId:   0,
			NodeType: planpb.Node_TABLE_SCAN,
			IndexScanInfo: planpb.IndexScanInfo{
				IsIndexScan: true,
				IndexName:   "Σ",
			},
		},
	}}}

	require.True(t, builder.indexAccessUsesIndex(0, "σ"))
	require.False(t, builder.indexAccessUsesIndex(0, "ς"))
}

func TestRecordIndexHintsMySQLCompatibility(t *testing.T) {
	tableDef := &planpb.TableDef{
		Name: "t",
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
		Indexes: []*planpb.IndexDef{
			{IndexName: "idx_alpha", TableExist: true},
			{IndexName: "idx_beta", TableExist: true},
		},
	}

	t.Run("unscoped hint applies to all scopes", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		err := builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
			{HintType: tree.HintUse, HintScope: tree.HintForScan, IndexNames: []string{"idx_al"}},
		})
		require.NoError(t, err)
		hintSet := builder.indexHintsByScan[1]
		require.True(t, hintSet.scan.useSpecified)
		require.True(t, hintSet.join.useSpecified)
		require.True(t, hintSet.order.useSpecified)
		require.True(t, hintSet.group.useSpecified)
		_, ok := hintSet.scan.use["idx_alpha"]
		require.True(t, ok)
		_, ok = hintSet.join.use["idx_alpha"]
		require.True(t, ok)
	})

	t.Run("force and ignore reject empty list", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		require.Error(t, builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
			{HintType: tree.HintForce, HintScope: tree.HintForJoin},
		}))
		require.Error(t, builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
			{HintType: tree.HintIgnore, HintScope: tree.HintForOrderBy},
		}))
	})

	t.Run("use and force conflict", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		err := builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
			{HintType: tree.HintUse, HintScope: tree.HintForJoin, IndexNames: []string{"idx_alpha"}},
			{HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_beta"}},
		})
		require.Error(t, err)
	})

	t.Run("use and force conflict across scopes", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		err := builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
			{HintType: tree.HintUse, HintScope: tree.HintForJoin, IndexNames: []string{"idx_alpha"}},
			{HintType: tree.HintForce, HintScope: tree.HintForOrderBy, IndexNames: []string{"idx_beta"}},
		})
		require.Error(t, err)
	})

	t.Run("ambiguous prefix", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		err := builder.recordIndexHints(1, tableDef, []*tree.IndexHint{
			{HintType: tree.HintUse, HintScope: tree.HintForJoin, IndexNames: []string{"idx_"}},
		})
		require.Error(t, err)
	})
}

func TestIndexHintAffectsRegularIndexChoice(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	plan, err := runOneStmt(mock, t, "select a from index_hint_t use index(idx_ab) where a = 1")
	require.NoError(t, err)
	require.Equal(t, "idx_ab", findFirstIndexScanName(plan))
}

func TestIndexHintUseEmptyDisablesRegularIndexChoice(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	plan, err := runOneStmt(mock, t, "select a from index_hint_t use index() where a = 1")
	require.NoError(t, err)
	require.Empty(t, findFirstIndexScanName(plan))
}

func TestIndexHintOrderScopeSelectsCoveringIndexWithoutFilter(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	queryPlan, err := runOneStmt(mock, t, "select a from index_hint_t force index for order by(idx_a) order by a limit 10")
	require.NoError(t, err)
	require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select a from index_hint_t ignore index for order by(idx_a) order by a limit 10")
	require.NoError(t, err)
	require.NotEqual(t, "idx_a", findFirstIndexScanName(queryPlan))
}

func TestForceIndexForOrderSQLCalcFoundRowsSkipsOrderedLimit(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	queryPlan, err := runOneStmt(mock, t,
		"select sql_calc_found_rows a from index_hint_t force index for order by(idx_a) where a = 1 order by a limit 1")
	require.NoError(t, err)
	indexScan := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, indexScan)
	require.Equal(t, "idx_a", indexScan.IndexScanInfo.IndexName)
	require.NotEmpty(t, indexScan.OrderBy)
	require.Nil(t, indexScan.IndexReaderParam)
}

func TestIndexHintOrderScopeKeepsFloatSortLogical(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	mock.ctxt.tables["index_hint_t"].Cols[1].Typ = planpb.Type{Id: int32(types.T_float64)}

	queryPlan, err := runOneStmt(mock, t,
		"select a from index_hint_t force index for order by(idx_a) order by a limit 10")
	require.NoError(t, err)
	indexScan := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, indexScan)
	require.Equal(t, "idx_a", indexScan.IndexScanInfo.IndexName)
	require.Empty(t, indexScan.OrderBy)
	require.Nil(t, indexScan.IndexReaderParam)
	require.True(t, planHasSort(queryPlan))
}

func TestIndexHintOrderScopePreservesCoveringIndexFilters(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	queryPlan, err := runOneStmt(mock, t, "select id,a,b from index_hint_t force index for order by(idx_ab) where b = 1 order by a limit 1")
	require.NoError(t, err)
	indexScan := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, indexScan)
	require.Equal(t, "idx_ab", indexScan.IndexScanInfo.IndexName)
	require.NotEmpty(t, indexScan.FilterList)
	require.Nil(t, indexScan.IndexReaderParam)
}

func TestForceIndexOrderAcceptsEqualityFixedLeadingPrefix(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		descending bool
		backfill   bool
	}{
		{
			name: "ascending covering without limit",
			sql: `select id, a, b from index_hint_t force index for order by(idx_ab)
				where a = 1 and b between 10 and 20 order by b, id`,
		},
		{
			name: "descending noncovering with limit",
			sql: `select payload from index_hint_t force index for order by(idx_ab)
				where a = 1 and b between 10 and 20 order by b desc, id desc limit 10`,
			descending: true,
			backfill:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			addIndexHintPayloadColumnForTest(mock)

			queryPlan, err := runOneStmt(mock, t, tt.sql)
			require.NoError(t, err)
			indexScan := findFirstIndexScanNode(queryPlan)
			require.NotNil(t, indexScan)
			require.Equal(t, "idx_ab", indexScan.IndexScanInfo.IndexName)
			require.NotEmpty(t, indexScan.OrderBy)
			require.Equal(t, tt.descending, indexScan.OrderBy[0].Flag&planpb.OrderBySpec_DESC != 0)
			require.Equal(t, tt.backfill, planHasIndexJoin(queryPlan))
			require.True(t, planHasSort(queryPlan))
		})
	}
}

func TestIndexOrderColumnsMatchEqualityFixedPrefix(t *testing.T) {
	const scanTag int32 = 41
	intType := planpb.Type{Id: int32(types.T_int32)}
	tableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: intType},
			{Name: "a", Typ: intType},
			{Name: "b", Typ: intType},
		},
		Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2},
	}

	literalEquality := func(commuted bool, relPos int32) *planpb.Expr {
		expr := makeEqFilterExpr(1)
		expr.GetF().Args[0].GetCol().RelPos = relPos
		if commuted {
			expr.GetF().Args[0], expr.GetF().Args[1] = expr.GetF().Args[1], expr.GetF().Args[0]
		}
		return expr
	}
	parameterEquality := makeEqFilterExpr(1)
	parameterEquality.GetF().Args[0].GetCol().RelPos = scanTag
	parameterEquality.GetF().Args[1] = &planpb.Expr{
		Typ:  intType,
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	columnEquality := makeEqFilterExpr(1)
	columnEquality.GetF().Args[0].GetCol().RelPos = scanTag
	columnEquality.GetF().Args[1] = GetColExpr(intType, scanTag, 2)
	nonEquality := makeEqFilterExpr(1)
	nonEquality.GetF().Func.ObjName = ">"
	nonEquality.GetF().Args[0].GetCol().RelPos = scanTag

	tests := []struct {
		name       string
		parts      []string
		filters    []*planpb.Expr
		orderCols  []int32
		compatible bool
	}{
		{name: "literal fixes leading part", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{literalEquality(false, scanTag)}, orderCols: []int32{2, 0}, compatible: true},
		{name: "commuted literal fixes leading part", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{literalEquality(true, scanTag)}, orderCols: []int32{2, 0}, compatible: true},
		{name: "parameter fixes leading part", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{parameterEquality}, orderCols: []int32{2, 0}, compatible: true},
		{name: "fixed part is order neutral", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{literalEquality(false, scanTag)}, orderCols: []int32{1, 2, 0}, compatible: true},
		{name: "order by fixed part only", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{literalEquality(false, scanTag)}, orderCols: []int32{1}, compatible: true},
		{name: "column equality does not fix a value", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{columnEquality}, orderCols: []int32{2, 0}},
		{name: "foreign binding does not fix scan column", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{literalEquality(false, scanTag+1)}, orderCols: []int32{2, 0}},
		{name: "non equality does not fix a value", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{nonEquality}, orderCols: []int32{2, 0}},
		{name: "unconstrained suffix cannot be skipped", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{literalEquality(false, scanTag)}, orderCols: []int32{0}},
		{name: "order cannot extend past index suffix", parts: []string{"a"}, filters: []*planpb.Expr{literalEquality(false, scanTag)}, orderCols: []int32{2}},
		{name: "missing index part metadata is not fixed", parts: []string{"missing", "b"}, filters: []*planpb.Expr{literalEquality(false, scanTag)}, orderCols: []int32{2}},
		{name: "invalid order column is rejected", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{literalEquality(false, scanTag)}, orderCols: []int32{3}},
		{name: "empty order is rejected", parts: []string{"a", "b", "id"}, filters: []*planpb.Expr{literalEquality(false, scanTag)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanNode := &planpb.Node{
				BindingTags: []int32{scanTag},
				TableDef:    tableDef,
				FilterList:  tt.filters,
			}
			idxDef := &planpb.IndexDef{Parts: tt.parts}
			require.Equal(t, tt.compatible, indexOrderColumnsMatch(idxDef, scanNode, tt.orderCols))
		})
	}
}

func TestPlainForceIndexRetainsAccessWhenOrderIsIncompatible(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		backfill bool
	}{
		{
			name: "in predicate fixes multiple leading values",
			sql:  "select id, a, b from index_hint_t force index(idx_ab) where a in (1, 2) order by b, id",
		},
		{
			name: "range predicate leaves leading part varying with limit",
			sql:  "select id, a, b from index_hint_t force index(idx_ab) where a between 1 and 2 order by b desc, id desc limit 10",
		},
		{
			name:     "unconstrained middle part noncovering",
			sql:      "select payload from index_hint_t force index(idx_ab) where a = 1 order by id",
			backfill: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			addIndexHintPayloadColumnForTest(mock)

			queryPlan, err := runOneStmt(mock, t, tt.sql)
			require.NoError(t, err)
			indexScan := findFirstIndexScanNode(queryPlan)
			require.NotNil(t, indexScan)
			require.Equal(t, "idx_ab", indexScan.IndexScanInfo.IndexName)
			require.Empty(t, indexScan.OrderBy)
			require.Equal(t, tt.backfill, planHasIndexJoin(queryPlan))
			require.True(t, planHasSort(queryPlan))
		})
	}
}

func TestForceIndexOrderIncompatibleControls(t *testing.T) {
	for _, tt := range []struct {
		name      string
		predicate string
	}{
		{name: "volatile right operand", predicate: "a = floor(rand() * 2)"},
		{name: "volatile left operand", predicate: "floor(rand() * 2) = a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			mock.ctxt.tables["index_hint_t"].Cols[1].Typ = planpb.Type{Id: int32(types.T_float64)}

			queryPlan, err := runOneStmt(mock, t,
				"select id, a, b from index_hint_t force index for order by(idx_ab) where "+tt.predicate+" order by b, id")
			require.NoError(t, err)
			require.Empty(t, findFirstIndexScanName(queryPlan))
			require.True(t, planHasSort(queryPlan))
		})
	}

	t.Run("order-scoped force does not become scan force", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)

		queryPlan, err := runOneStmt(mock, t,
			"select id from index_hint_t force index for order by(idx_ab) where a = 1 order by id")
		require.NoError(t, err)
		require.Empty(t, findFirstIndexScanName(queryPlan))
		require.True(t, planHasSort(queryPlan))
	})

	t.Run("ordinary optimizer remains unforced", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)

		queryPlan, err := runOneStmt(mock, t,
			"select id from index_hint_t where b = 1 order by b, id")
		require.NoError(t, err)
		require.Empty(t, findFirstIndexScanName(queryPlan))
		require.True(t, planHasSort(queryPlan))
	})

	t.Run("invalid plain force still errors", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)

		_, err := runOneStmt(mock, t,
			"select id from index_hint_t force index(idx_missing) where a = 1 order by b, id")
		require.Error(t, err)
		var moErr *moerr.Error
		require.ErrorAs(t, err, &moErr)
		require.Equal(t, moerr.ER_KEY_DOES_NOT_EXIST, moErr.MySQLCode())
	})
}

func TestIgnoreIndexForOrderByBlocksCoveringIndexOrderedRead(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	queryPlan, err := runOneStmt(mock, t, `
		select id, a
		from index_hint_t ignore index for order by (idx_a)
		where a = 1
		order by id
		limit 10`)
	require.NoError(t, err)
	indexScan := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, indexScan)
	require.Equal(t, "idx_a", indexScan.IndexScanInfo.IndexName)
	require.Nil(t, indexScan.IndexReaderParam)
	require.Empty(t, indexScan.OrderBy)
	require.False(t, hasMessageType(indexScan.RecvMsgList, int32(message.MsgTopValue)))
	for _, node := range queryPlan.GetQuery().Nodes {
		require.False(t, hasMessageType(node.SendMsgList, int32(message.MsgTopValue)))
	}

	queryPlan, err = runOneStmt(mock, t, `
		select id, a
		from index_hint_t
		where a = 1
		order by id
		limit 10`)
	require.NoError(t, err)
	indexScan = findFirstIndexScanNode(queryPlan)
	require.NotNil(t, indexScan)
	require.Equal(t, "idx_a", indexScan.IndexScanInfo.IndexName)
	require.NotNil(t, indexScan.IndexReaderParam)
	require.NotEmpty(t, indexScan.OrderBy)
	require.True(t, hasMessageType(indexScan.RecvMsgList, int32(message.MsgTopValue)))
}

func hasMessageType(messages []planpb.MsgHeader, msgType int32) bool {
	for _, msg := range messages {
		if msg.MsgType == msgType {
			return true
		}
	}
	return false
}

func TestIndexHintOrderScopeBuildsNonCoveringBackfillJoin(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	queryPlan, err := runOneStmt(mock, t, "select b from index_hint_t force index for order by(idx_a) order by a")
	require.NoError(t, err)
	require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))
	require.True(t, planHasIndexJoin(queryPlan))
}

func TestIndexHintOrderScopeFindsScanBelowJoin(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	queryPlan, err := runOneStmt(mock, t, "select t1.a from index_hint_t t1 force index for order by(idx_a) join index_hint_t t2 on t1.id = t2.id order by t1.a")
	require.NoError(t, err)
	require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))
}

func TestIndexHintPrimaryOrderAndGroupScopes(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	queryPlan, err := runOneStmt(mock, t, "select id from index_hint_t force index for order by(primary) order by id")
	require.NoError(t, err)
	require.Empty(t, findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select id from index_hint_t ignore index for order by(primary) order by id")
	require.NoError(t, err)
	require.Empty(t, findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select id,count(*) from index_hint_t ignore index for group by(primary) group by id")
	require.NoError(t, err)
	require.Empty(t, findFirstIndexScanName(queryPlan))
}

func TestIndexHintGroupScopeSelectsAndIgnoresCoveringIndex(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	queryPlan, err := runOneStmt(mock, t, "select a, count(*) from index_hint_t where a = 1 group by a")
	require.NoError(t, err)
	require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select a, count(*) from index_hint_t use index for group by(idx_ab) where a = 1 group by a")
	require.NoError(t, err)
	require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select a, count(*) from index_hint_t ignore index for group by(idx_a) where a = 1 group by a")
	require.NoError(t, err)
	require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select a, count(*) from index_hint_t force index for group by(idx_a) group by a")
	require.NoError(t, err)
	require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select a, count(*) from index_hint_t ignore index for group by(idx_a) group by a")
	require.NoError(t, err)
	require.NotEqual(t, "idx_a", findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select a, count(*) from index_hint_t force index for group by(idx_ab) where b = 1 group by a")
	require.NoError(t, err)
	indexScan := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, indexScan)
	require.Equal(t, "idx_ab", indexScan.IndexScanInfo.IndexName)
	require.NotEmpty(t, indexScan.FilterList)

	queryPlan, err = runOneStmt(mock, t, "select a, max(b) from index_hint_t force index for group by(idx_a) group by a")
	require.NoError(t, err)
	require.Equal(t, "idx_a", findFirstIndexScanName(queryPlan))
	require.True(t, planHasIndexJoin(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select a,b from index_hint_t force index for order by(uk_ab) order by a,b")
	require.NoError(t, err)
	require.Equal(t, "uk_ab", findFirstIndexScanName(queryPlan))

	queryPlan, err = runOneStmt(mock, t, "select a,b,count(*) from index_hint_t force index for group by(uk_ab) group by a,b")
	require.NoError(t, err)
	require.Equal(t, "uk_ab", findFirstIndexScanName(queryPlan))
}

func TestSecondaryIndexHiddenDependenciesSurviveGroupedJoinRemap(t *testing.T) {
	mock := NewMockOptimizer(true)
	addGroupedJoinIndexTablesForTest(mock)

	tests := []struct {
		name      string
		sql       string
		wantIndex bool
	}{
		{
			name: "indexed left grouped",
			sql: `
				select p.tenant_id, p.state, count(*), sum(c.weight)
				from grouped_join_parent p force index(idx_tenant_state_id)
				join grouped_join_child c
					on c.tenant_id = p.tenant_id and c.id = p.id
				where p.tenant_id between 3 and 9
					and p.state in ('READY', 'HOLD')
					and c.tag_id = 1
				group by p.tenant_id, p.state`,
			wantIndex: true,
		},
		{
			name: "indexed left grouped with having and order",
			sql: `
				select p.tenant_id, p.state, count(*), sum(c.weight)
				from grouped_join_parent p force index(idx_tenant_state_id)
				join grouped_join_child c
					on c.tenant_id = p.tenant_id and c.id = p.id
				where p.tenant_id between 3 and 9
					and p.state in ('READY', 'HOLD')
					and c.tag_id = 1
				group by p.tenant_id, p.state
				having sum(c.weight) > 0
				order by p.tenant_id, p.state`,
			wantIndex: true,
		},
		{
			name: "indexed right grouped",
			sql: `
				select p.tenant_id, p.state, count(*), sum(c.weight)
				from grouped_join_child c
				join grouped_join_parent p force index(idx_tenant_state_id)
					on p.tenant_id = c.tenant_id and p.id = c.id
				where p.tenant_id between 3 and 9
					and p.state in ('READY', 'HOLD')
					and c.tag_id = 1
				group by p.tenant_id, p.state`,
			wantIndex: true,
		},
		{
			name: "indexed left scalar aggregate control",
			sql: `
				select count(*), sum(c.weight)
				from grouped_join_parent p force index(idx_tenant_state_id)
				join grouped_join_child c
					on c.tenant_id = p.tenant_id and c.id = p.id
				where p.tenant_id between 3 and 9
					and p.state in ('READY', 'HOLD')
					and c.tag_id = 1`,
			wantIndex: true,
		},
		{
			name: "indexed left raw rows control",
			sql: `
				select p.tenant_id, p.state, c.weight
				from grouped_join_parent p force index(idx_tenant_state_id)
				join grouped_join_child c
					on c.tenant_id = p.tenant_id and c.id = p.id
				where p.tenant_id between 3 and 9
					and p.state in ('READY', 'HOLD')
					and c.tag_id = 1`,
			wantIndex: true,
		},
		{
			name: "ignored index grouped control",
			sql: `
				select p.tenant_id, p.state, count(*), sum(c.weight)
				from grouped_join_parent p ignore index(idx_tenant_state_id)
				join grouped_join_child c
					on c.tenant_id = p.tenant_id and c.id = p.id
				where p.tenant_id between 3 and 9
					and p.state in ('READY', 'HOLD')
					and c.tag_id = 1
				group by p.tenant_id, p.state`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queryPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			indexNames := reachableIndexScanNames(queryPlan.GetQuery())
			if test.wantIndex {
				require.Contains(t, indexNames, "idx_tenant_state_id")
			} else {
				require.NotContains(t, indexNames, "idx_tenant_state_id")
			}
		})
	}
}

func TestGroupedJoinForceIndexCandidateClosure(t *testing.T) {
	tests := []struct {
		name        string
		hints       string
		selectList  string
		tail        string
		wantIndexes []string
		notIndexes  []string
	}{
		{
			name:        "group access matches later forced candidate",
			hints:       "force index(idx_state, idx_tenant_state_id)",
			selectList:  "p.tenant_id, p.state, count(*), sum(c.weight)",
			tail:        "group by p.tenant_id, p.state",
			wantIndexes: []string{"idx_tenant_state_id"},
			notIndexes:  []string{"idx_state"},
		},
		{
			name: "join force wins over incompatible group force",
			hints: `force index for group by(idx_tenant_state_id)
				force index for join(idx_state)`,
			selectList:  "p.tenant_id, p.state, count(*), sum(c.weight)",
			tail:        "group by p.tenant_id, p.state",
			wantIndexes: []string{"idx_state"},
			notIndexes:  []string{"idx_tenant_state_id"},
		},
		{
			name: "forced primary join wins over secondary group access",
			hints: `force index for group by(idx_tenant_state_id)
				force index for join(primary)`,
			selectList: "p.tenant_id, p.state, count(*), sum(c.weight)",
			tail:       "group by p.tenant_id, p.state",
			notIndexes: []string{"idx_state", "idx_tenant_state_id"},
		},
		{
			name: "forced secondary join wins over primary group access",
			hints: `force index for group by(primary)
				force index for join(idx_state)`,
			selectList:  "p.tenant_id, p.state, count(*), sum(c.weight)",
			tail:        "group by p.tenant_id, p.state",
			wantIndexes: []string{"idx_state"},
			notIndexes:  []string{"idx_tenant_state_id"},
		},
		{
			name: "primary group access matches allowed join candidate",
			hints: `force index for group by(primary)
				force index for join(primary, idx_state)`,
			selectList: "p.tenant_id, p.id, count(*), sum(c.weight)",
			tail:       "group by p.tenant_id, p.id",
			notIndexes: []string{"idx_state", "idx_tenant_state_id"},
		},
		{
			name:        "order access matches later forced candidate",
			hints:       "force index(idx_state, idx_tenant_state_id)",
			selectList:  "p.tenant_id, p.state, c.weight",
			tail:        "order by p.tenant_id, p.state",
			wantIndexes: []string{"idx_tenant_state_id"},
			notIndexes:  []string{"idx_state"},
		},
		{
			name: "join force wins over incompatible order force",
			hints: `force index for order by(idx_tenant_state_id)
				force index for join(idx_state)`,
			selectList:  "p.tenant_id, p.state, c.weight",
			tail:        "order by p.tenant_id, p.state",
			wantIndexes: []string{"idx_state"},
			notIndexes:  []string{"idx_tenant_state_id"},
		},
		{
			name: "primary order access matches allowed join candidate",
			hints: `force index for order by(primary)
				force index for join(primary, idx_state)`,
			selectList: "p.tenant_id, p.id, c.weight",
			tail:       "order by p.tenant_id, p.id",
			notIndexes: []string{"idx_state", "idx_tenant_state_id"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addGroupedJoinIndexTablesForTest(mock)
			addGroupedJoinAlternativeIndexForTest(mock)
			queryPlan, err := runOneStmt(mock, t, fmt.Sprintf(`
				select %s
				from grouped_join_parent p %s
				join grouped_join_child c
					on c.tenant_id = p.tenant_id and c.id = p.id
				where p.tenant_id between 3 and 9
					and p.state in ('READY', 'HOLD')
					and c.tag_id = 1
				%s`, test.selectList, test.hints, test.tail))
			require.NoError(t, err)
			indexNames := reachableIndexScanNames(queryPlan.GetQuery())
			for _, indexName := range test.wantIndexes {
				require.Contains(t, indexNames, indexName)
			}
			for _, indexName := range test.notIndexes {
				require.NotContains(t, indexNames, indexName)
			}
		})
	}
}

func TestIndexHintRejectsInvalidCombinations(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)

	_, err := runOneStmt(mock, t, "select a from index_hint_t force index() where a = 1")
	require.Error(t, err)
	_, err = runOneStmt(mock, t, "select a from index_hint_t ignore index() where a = 1")
	require.Error(t, err)
	_, err = runOneStmt(mock, t, "select a from index_hint_t use index(idx_a) force index(idx_ab) where a = 1")
	require.Error(t, err)
	_, err = runOneStmt(mock, t, "select a from index_hint_t use index(idx_) where a = 1")
	require.Error(t, err)
}

type indexHintResolveFailureContext struct {
	*MockCompilerContext
	nilMetadata bool
}

func (c *indexHintResolveFailureContext) ResolveIndexTableByRef(_ *ObjectRef, _ string, _ *Snapshot) (*ObjectRef, *TableDef, error) {
	if c.nilMetadata {
		return nil, nil, nil
	}
	return nil, nil, moerr.NewInternalErrorNoCtx("injected index metadata failure")
}

func TestHintedIndexAccessReturnsMetadataErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		nilMetadata bool
	}{
		{name: "resolve error"},
		{name: "nil metadata", nilMetadata: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &indexHintResolveFailureContext{MockCompilerContext: NewMockCompilerContext(true), nilMetadata: tc.nilMetadata}
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
			tag := builder.genNewBindTag()
			idxDef := &planpb.IndexDef{
				IndexName: "idx_a", IndexTableName: "idx_a_table", Parts: []string{"a", catalog.CreateAlias("id")}, TableExist: true,
			}
			tableDef := &planpb.TableDef{
				Name: "t",
				Cols: []*planpb.ColDef{
					{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
					{Name: "a", Typ: planpb.Type{Id: int32(types.T_int32)}},
				},
				Name2ColIndex: map[string]int32{"id": 0, "a": 1},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				Indexes:       []*planpb.IndexDef{idxDef},
			}
			scanNode := &planpb.Node{
				NodeId: 1, NodeType: planpb.Node_TABLE_SCAN, TableDef: tableDef,
				ObjRef: &planpb.ObjectRef{ObjName: "t"}, BindingTags: []int32{tag},
			}
			idxColMap := make(map[[2]int32]*planpb.Expr)
			_, _, _, err := builder.tryHintedIndexAccess(idxDef, scanNode, map[[2]int32]int{{tag, 1}: 1}, idxColMap)
			require.Error(t, err)
			require.Empty(t, builder.qry.Nodes)
			require.Empty(t, idxColMap)
		})
	}
}

func TestIndexHintJoinScopeFiltersCandidates(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ignore      bool
		force       bool
		selectivity float64
		outcnt      float64
		wantRewrite bool
	}{
		{name: "control rewrites", wantRewrite: true},
		{name: "join ignore prevents rewrite", ignore: true, wantRewrite: false},
		{name: "high selectivity skips without force", selectivity: 0.8},
		{name: "force bypasses selectivity gate", force: true, selectivity: 0.8, wantRewrite: true},
		{name: "high outcnt skips without force", outcnt: 1000},
		{name: "force bypasses outcnt gate", force: true, outcnt: 1000, wantRewrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, joinID, leftScanID, leftDef := makeIndexHintJoinBuilder(t)
			rightScan := builder.qry.Nodes[builder.qry.Nodes[joinID].Children[1]]
			if tc.selectivity > 0 {
				rightScan.Stats.Selectivity = tc.selectivity
			}
			if tc.outcnt > 0 {
				rightScan.Stats.Outcnt = tc.outcnt
			}
			if tc.ignore {
				err := builder.recordIndexHints(leftScanID, leftDef, []*tree.IndexHint{
					{HintType: tree.HintIgnore, HintScope: tree.HintForJoin, IndexNames: []string{"idx_a"}},
				})
				require.NoError(t, err)
			}
			if tc.force {
				err := builder.recordIndexHints(leftScanID, leftDef, []*tree.IndexHint{
					{HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_a"}},
				})
				require.NoError(t, err)
			}
			newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			require.Equal(t, joinID, newID)
			if tc.wantRewrite {
				require.NotEqual(t, leftScanID, builder.qry.Nodes[joinID].Children[0])
				require.Equal(t, planpb.Node_INDEX, builder.qry.Nodes[builder.qry.Nodes[joinID].Children[0]].JoinType)
			} else {
				require.Equal(t, leftScanID, builder.qry.Nodes[joinID].Children[0])
			}
		})
	}
}

func TestApplyIndicesForJoinsSkipsNilIndexMetadata(t *testing.T) {
	builder, joinID, leftScanID, leftDef := makeIndexHintJoinBuilder(t)
	leftDef.Indexes = append([]*planpb.IndexDef{nil}, leftDef.Indexes...)

	newID, err := builder.applyIndicesForJoins(
		joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	join := builder.qry.Nodes[joinID]
	require.NotEqual(t, leftScanID, join.Children[0])
	require.Equal(t, planpb.Node_INDEX, builder.qry.Nodes[join.Children[0]].JoinType)
	require.Len(t, join.RuntimeFilterBuildList, 1)
}

func TestIndexJoinSkipsLossyPrefixIndex(t *testing.T) {
	builder, joinID, leftScanID, leftDef := makeIndexHintJoinBuilder(t)
	leftDef.Indexes[0].IndexAlgoParams = `{"prefix_lengths":"a:1"}`
	join := builder.qry.Nodes[joinID]

	newID, err := builder.applyIndicesForJoins(
		joinID, join, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, leftScanID, join.Children[0])
	require.Empty(t, join.RuntimeFilterBuildList)
}

func TestIndexJoinBuildsVersionedSerializedRuntimeFilter(t *testing.T) {
	builder, joinID, leftScanID, _ := makeIndexHintJoinBuilder(t)
	join := builder.qry.Nodes[joinID]

	newID, err := builder.applyIndicesForJoins(
		joinID, join, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.NotEqual(t, leftScanID, join.Children[0])
	require.Len(t, join.RuntimeFilterBuildList, 1)

	buildSpec := join.RuntimeFilterBuildList[0]
	require.Equal(t,
		planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1,
		buildSpec.KeyEncoding)
	require.True(t, buildSpec.MatchPrefix)
	require.Equal(t,
		[]planpb.Type{{Id: int32(types.T_int32)}},
		buildSpec.KeyComponentProbeTypes)
	require.Nil(t, buildSpec.Expr)
	require.Equal(t, int32(0),
		buildSpec.BuildExpr.GetF().Args[0].GetCol().ColPos)

	indexJoin := builder.qry.Nodes[join.Children[0]]
	require.Equal(t, planpb.Node_INDEX, indexJoin.JoinType)
	indexScan := builder.qry.Nodes[indexJoin.Children[1]]
	require.Len(t, indexScan.RuntimeFilterProbeList, 1)
	require.Equal(t, buildSpec.Tag, indexScan.RuntimeFilterProbeList[0].Tag)
}

func TestEnumIndexJoinsRemainEligible(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion8)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	enumType := planpb.Type{
		Id:         int32(types.T_enum),
		Enumvalues: "small,large",
	}
	setJoinKeyType := func(
		builder *QueryBuilder,
		join *planpb.Node,
		leftDef *planpb.TableDef,
	) {
		right := builder.qry.Nodes[join.Children[1]]
		leftDef.Cols[1].Typ = enumType
		right.TableDef.Cols[1].Typ = enumType
		join.OnList[0].GetF().Args[0].Typ = enumType
		join.OnList[0].GetF().Args[1].Typ = enumType
	}

	t.Run("serialized secondary index", func(t *testing.T) {
		builder, joinID, leftScanID, leftDef :=
			makeIndexHintJoinBuilder(t)
		join := builder.qry.Nodes[joinID]
		setJoinKeyType(builder, join, leftDef)

		_, err := builder.applyIndicesForJoins(
			joinID, join, map[[2]int32]int{},
			map[[2]int32]*planpb.Expr{})
		require.NoError(t, err)
		require.NotEqual(t, leftScanID, join.Children[0])
		require.Len(t, join.RuntimeFilterBuildList, 1)
		spec := join.RuntimeFilterBuildList[0]
		require.Equal(t,
			planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1,
			spec.KeyEncoding)
		require.Equal(t, []planpb.Type{enumType},
			spec.KeyComponentProbeTypes)
	})

	t.Run("direct unique index", func(t *testing.T) {
		builder, joinID, leftScanID, leftDef :=
			makeIndexHintJoinBuilder(t)
		join := builder.qry.Nodes[joinID]
		setJoinKeyType(builder, join, leftDef)
		leftDef.Indexes = []*planpb.IndexDef{{
			IndexName:      "uidx_a",
			IndexTableName: "idx_join_a_table",
			Parts:          []string{"a"},
			Unique:         true,
			TableExist:     true,
		}}
		mockCtx := builder.compCtx.(*fullTextJoinMockCompilerContext)
		mockCtx.tables["idx_join_a_table"].Cols[0].Typ = enumType

		_, err := builder.applyIndicesForJoins(
			joinID, join, map[[2]int32]int{},
			map[[2]int32]*planpb.Expr{})
		require.NoError(t, err)
		require.NotEqual(t, leftScanID, join.Children[0])
		require.Len(t, join.RuntimeFilterBuildList, 1)
		spec := join.RuntimeFilterBuildList[0]
		require.Equal(t,
			planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
			spec.KeyEncoding)
		require.Equal(t, int32(types.T_enum), spec.ProbeType.Id)
		require.Equal(t, int32(types.T_enum), spec.BuildExpr.Typ.Id)
	})
}

func TestIndexJoinSkipsUnsafeSerializedRuntimeFilter(t *testing.T) {
	builder, joinID, leftScanID, leftDef := makeIndexHintJoinBuilder(t)
	join := builder.qry.Nodes[joinID]
	right := builder.qry.Nodes[join.Children[1]]
	floatType := planpb.Type{Id: int32(types.T_float64)}
	leftDef.Cols[1].Typ = floatType
	right.TableDef.Cols[1].Typ = floatType
	join.OnList[0].GetF().Args[0].Typ = floatType
	join.OnList[0].GetF().Args[1].Typ = floatType
	nodeCount := len(builder.qry.Nodes)
	statsBefore := DeepCopyStats(builder.qry.Nodes[leftScanID].Stats)

	newID, err := builder.applyIndicesForJoins(
		joinID, join, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, leftScanID, join.Children[0])
	require.Empty(t, join.RuntimeFilterBuildList)
	require.Equal(t, nodeCount, len(builder.qry.Nodes))
	require.Equal(t, statsBefore, builder.qry.Nodes[leftScanID].Stats)
}

func TestIndexJoinSerializedRuntimeFilterUsesCompactedHashSlot(t *testing.T) {
	builder, joinID, _, _ := makeIndexHintJoinBuilder(t)
	join := builder.qry.Nodes[joinID]
	join.OnList = append(
		[]*planpb.Expr{MakePlan2BoolConstExprWithType(true)},
		join.OnList...,
	)

	_, err := builder.applyIndicesForJoins(
		joinID, join, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Len(t, join.RuntimeFilterBuildList, 1)
	require.Equal(t, int32(0),
		join.RuntimeFilterBuildList[0].
			BuildExpr.GetF().Args[0].GetCol().ColPos,
		"residual OnList predicates must not shift compact HashBuild key slots")
}

func TestUniqueIndexRuntimeFilterUsesSelectedHashSlot(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion8)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	builder, joinID, _, leftDef := makeIndexHintJoinBuilder(t)
	leftDef.Indexes = []*planpb.IndexDef{{
		IndexName:      "uidx_b",
		IndexTableName: "idx_join_b_table",
		Parts:          []string{"b"},
		Unique:         true,
		TableExist:     true,
	}}
	mockCtx := builder.compCtx.(*fullTextJoinMockCompilerContext)
	mockCtx.tables["idx_join_b_table"].Cols[0].Typ =
		planpb.Type{Id: int32(types.T_int32)}

	join := builder.qry.Nodes[joinID]
	leftTag := builder.qry.Nodes[join.Children[0]].BindingTags[0]
	right := builder.qry.Nodes[join.Children[1]]
	rightTag := right.BindingTags[0]
	// The existing a=b predicate occupies HashBuild slot 0. The unique index
	// matches this second b=a predicate and must therefore publish slot 1.
	join.OnList = append(join.OnList, ftjMakeEqExpr(
		t,
		ftjColExpr(leftDef, leftTag, 2),
		ftjColExpr(right.TableDef, rightTag, 0),
	))

	_, err := builder.applyIndicesForJoins(
		joinID, join, map[[2]int32]int{},
		map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Len(t, join.RuntimeFilterBuildList, 1)
	spec := DeepCopyRuntimeFilterSpec(join.RuntimeFilterBuildList[0])
	require.Equal(t,
		planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
		spec.KeyEncoding)
	require.Equal(t, int32(1), spec.BuildExpr.GetCol().ColPos)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	first := testutil.MakeInt32Vector([]int32{901, 902, 903}, nil, proc.Mp())
	selected := testutil.MakeInt32Vector([]int32{11, 12, 13}, nil, proc.Mp())
	input := batch.NewWithSize(2)
	input.Vecs[0] = first
	input.Vecs[1] = selected
	input.SetRowCount(3)
	child := colexec.NewMockOperator().WithBatchs(
		[]*batch.Batch{input, nil})
	arg := hashbuild.NewArgument()
	arg.JoinMapTag = 9002
	arg.JoinMapRefCnt = 1
	arg.NeedHashMap = true
	arg.Conditions = []*planpb.Expr{
		GetColExpr(planpb.Type{Id: int32(types.T_int32)}, 0, 0),
		GetColExpr(planpb.Type{Id: int32(types.T_int32)}, 0, 1),
	}
	arg.RuntimeFilterSpec = spec
	arg.AppendChild(child)
	registry, account := installIndexPlanHashBuildAllocation(t, arg)
	require.NoError(t, child.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard(),
	)
	msgs, done, err := receiver.ReceiveMessage(false, proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)
	payload := vector.NewVec(types.T_any.ToType())
	require.NoError(t, payload.UnmarshalBinary(runtimeFilter.Data))
	require.Equal(t, []int32{11, 12, 13},
		vector.MustFixedColNoTypeCheck[int32](payload))

	payload.Free(proc.Mp())
	runtimeFilter.Destroy()
	arg.Free(proc, false, nil)
	proc.GetMessageBoard().Reset()
	finishIndexPlanHashBuildAllocation(t, registry, account, arg)
	child.Free(proc, false, nil)
	arg.Release()
	child.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestIndexJoinGeneratedSerializedRuntimeFilterExecutesEndToEnd(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion8)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	builder, joinID, _, _ := makeIndexHintJoinBuilder(t)
	join := builder.qry.Nodes[joinID]
	join.OnList = append(
		[]*planpb.Expr{MakePlan2BoolConstExprWithType(true)},
		join.OnList...,
	)
	_, err := builder.applyIndicesForJoins(
		joinID, join, map[[2]int32]int{},
		map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Len(t, join.RuntimeFilterBuildList, 1)
	spec := DeepCopyRuntimeFilterSpec(join.RuntimeFilterBuildList[0])
	require.Nil(t, spec.Expr)
	require.NotNil(t, spec.BuildExpr)
	require.True(t, spec.MatchPrefix)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	values := make([]int32, 37)
	for i := range values {
		values[i] = int32(i*17 - 101)
	}
	inputVec := testutil.MakeInt32Vector(values, nil, proc.Mp())
	input := batch.NewWithSize(1)
	input.Vecs[0] = inputVec
	input.SetRowCount(len(values))

	encoder, err := function.NewSerialValueEncoder(inputVec)
	require.NoError(t, err)
	packer := types.NewPacker()
	expected := make(map[string]struct{}, len(values))
	fullKeys := vector.NewVec(types.T_varchar.ToType())
	for i := range values {
		packer.Reset()
		encoder(inputVec, i, packer)
		expected[string(packer.GetBuf())] = struct{}{}
		packer.EncodeInt64(int64(i + 1))
		require.NoError(t, vector.AppendBytes(
			fullKeys, packer.GetBuf(), false, proc.Mp()))
	}
	packer.Reset()
	packer.EncodeInt32(999_999)
	packer.EncodeInt64(1)
	require.NoError(t, vector.AppendBytes(
		fullKeys, packer.GetBuf(), false, proc.Mp()))
	packer.Close()

	child := colexec.NewMockOperator().WithBatchs(
		[]*batch.Batch{input, nil})
	arg := hashbuild.NewArgument()
	arg.JoinMapTag = 9001
	arg.JoinMapRefCnt = 1
	arg.NeedHashMap = true
	arg.HashOnPK = true
	arg.Conditions = []*planpb.Expr{GetColExpr(
		planpb.Type{Id: int32(types.T_int32)}, 0, 0)}
	arg.RuntimeFilterSpec = spec
	arg.AppendChild(child)
	registry, account := installIndexPlanHashBuildAllocation(t, arg)
	require.NoError(t, child.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard(),
	)
	msgs, done, err := receiver.ReceiveMessage(false, proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)
	require.Equal(t, int32(len(values)), runtimeFilter.Card)

	payload := vector.NewVec(types.T_any.ToType())
	require.NoError(t, payload.UnmarshalBinary(runtimeFilter.Data))
	require.Equal(t, len(values), payload.Length())
	for i := 0; i < payload.Length(); i++ {
		key := string(payload.GetBytesAt(i))
		_, exists := expected[key]
		require.Truef(t, exists, "unexpected serialized key at row %d", i)
		delete(expected, key)
	}
	require.Empty(t, expected)

	fullBatch := batch.NewWithSize(1)
	fullBatch.Vecs[0] = fullKeys
	fullBatch.SetRowCount(fullKeys.Length())
	inExpr := MakeInExpr(
		proc.Ctx,
		GetColExpr(
			planpb.Type{Id: int32(types.T_varchar)}, 0, 0),
		runtimeFilter.Card,
		runtimeFilter.Data,
		true,
	)
	matches, freeMatches, err := colexec.GetReadonlyResultFromExpression(
		proc, inExpr, []*batch.Batch{fullBatch})
	require.NoError(t, err)
	matchValues := vector.MustFixedColNoTypeCheck[bool](matches)
	require.Len(t, matchValues, len(values)+1)
	for i := range values {
		require.Truef(t, matchValues[i],
			"generated prefix filter rejected row %d", i)
	}
	require.False(t, matchValues[len(values)])
	freeMatches()

	fullBatch.Clean(proc.Mp())
	payload.Free(proc.Mp())
	runtimeFilter.Destroy()
	arg.Free(proc, false, nil)
	proc.GetMessageBoard().Reset()
	finishIndexPlanHashBuildAllocation(t, registry, account, arg)
	child.Free(proc, false, nil)
	arg.Release()
	child.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func installIndexPlanHashBuildAllocation(
	t testing.TB,
	arg *hashbuild.HashBuild,
) (*mpool.AllocationAccountRegistry, *mpool.AllocationAccount) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.Open(1 << 60)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	return registry, account
}

func finishIndexPlanHashBuildAllocation(
	t testing.TB,
	registry *mpool.AllocationAccountRegistry,
	account *mpool.AllocationAccount,
	arg *hashbuild.HashBuild,
) {
	t.Helper()
	require.NoError(t, arg.ClearAllocationAccount(account))
	snapshot, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
}

func TestForceIndexForJoinBuildsRightAccessWithoutReorder(t *testing.T) {
	builder, joinID, leftScanID, _ := makeIndexHintJoinBuilder(t)
	joinNode := builder.qry.Nodes[joinID]
	rightScanID := joinNode.Children[1]
	rightScan := builder.qry.Nodes[rightScanID]
	rightScan.TableDef.Indexes = []*planpb.IndexDef{{
		IndexName:      "idx_b",
		IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
		IndexTableName: "idx_join_b_table",
		Parts:          []string{"b", catalog.CreateAlias("a")},
		TableExist:     true,
	}}
	rightScan.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	rightScan.ObjRef = &planpb.ObjectRef{ObjName: "right_t"}
	rightScan.Stats = &planpb.Stats{TableCnt: 1000, Cost: 1000}

	err := builder.recordIndexHints(rightScanID, rightScan.TableDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_b"},
	}})
	require.NoError(t, err)

	newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, leftScanID, joinNode.Children[0])
	require.NotEqual(t, rightScanID, joinNode.Children[1])
	indexJoin := builder.qry.Nodes[joinNode.Children[1]]
	require.Equal(t, planpb.Node_INDEX, indexJoin.JoinType)
	require.Equal(t, rightScanID, indexJoin.Children[0])
}

func TestForceIndexForJoinReplacesFilterIndexWrapper(t *testing.T) {
	builder, joinID, _, _ := makeIndexHintJoinBuilder(t)
	joinNode := builder.qry.Nodes[joinID]
	rightScanID := joinNode.Children[1]
	rightScan := builder.qry.Nodes[rightScanID]
	rightScan.TableDef.Indexes = []*planpb.IndexDef{
		{IndexName: "idx_filter", IndexTableName: "idx_join_a_table", Parts: []string{"a", catalog.CreateAlias("b")}, TableExist: true},
		{IndexName: "idx_join", IndexTableName: "idx_join_b_table", Parts: []string{"b", catalog.CreateAlias("a")}, TableExist: true},
	}
	rightScan.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	rightScan.ObjRef = &planpb.ObjectRef{ObjName: "right_t"}
	require.NoError(t, builder.recordIndexHints(rightScanID, rightScan.TableDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_join"},
	}}))

	filterIndexScanID := int32(len(builder.qry.Nodes))
	builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
		NodeId:   filterIndexScanID,
		NodeType: planpb.Node_TABLE_SCAN,
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan: true, IndexName: "idx_filter", BelongToTable: "right_t",
		},
	})
	builder.ctxByNode = append(builder.ctxByNode, builder.ctxByNode[joinID])
	filterWrapperID := int32(len(builder.qry.Nodes))
	builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
		NodeId:   filterWrapperID,
		NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INDEX,
		Children: []int32{rightScanID, filterIndexScanID},
	})
	builder.ctxByNode = append(builder.ctxByNode, builder.ctxByNode[joinID])
	joinNode.Children[1] = filterWrapperID

	newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.NotEqual(t, filterWrapperID, joinNode.Children[1])
	forcedWrapper := builder.qry.Nodes[joinNode.Children[1]]
	require.Equal(t, planpb.Node_INDEX, forcedWrapper.JoinType)
	forcedIndexScan := builder.qry.Nodes[forcedWrapper.Children[1]]
	require.Equal(t, "idx_join", forcedIndexScan.IndexScanInfo.IndexName)
}

func TestForcePrimaryForJoinReplacesSecondaryAccess(t *testing.T) {
	builder, joinID, _, _ := makeIndexHintJoinBuilder(t)
	joinNode := builder.qry.Nodes[joinID]
	rightScanID := joinNode.Children[1]
	rightScan := builder.qry.Nodes[rightScanID]
	rightScan.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	rightScan.TableDef.Indexes = []*planpb.IndexDef{{
		IndexName: "idx_filter", IndexTableName: "idx_join_a_table",
		Parts: []string{"a", catalog.CreateAlias("a")}, TableExist: true,
	}}
	require.NoError(t, builder.recordIndexHints(rightScanID, rightScan.TableDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{PrimaryKeyName},
	}}))

	secondaryScanID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		Stats:    DefaultStats(),
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan: true,
			IndexName:   "idx_filter",
		},
	}, builder.ctxByNode[joinID])
	secondaryAccessID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_INDEX,
		Children: []int32{rightScanID, secondaryScanID},
	}, builder.ctxByNode[joinID])
	joinNode.Children[1] = secondaryAccessID

	newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, rightScanID, joinNode.Children[1])
}

func TestForceIndexForJoinPreservesMatchingIndexAccess(t *testing.T) {
	builder, joinID, _, _ := makeIndexHintJoinBuilder(t)
	joinNode := builder.qry.Nodes[joinID]
	rightScanID := joinNode.Children[1]
	rightScan := builder.qry.Nodes[rightScanID]
	rightScan.TableDef.Indexes = []*planpb.IndexDef{
		{
			IndexName:      "idx_a",
			IndexTableName: "idx_join_a_table",
			Parts:          []string{"a", catalog.CreateAlias("a")},
			TableExist:     true,
		},
		{
			IndexName:      "idx_b",
			IndexTableName: "idx_join_b_table",
			Parts:          []string{"b", catalog.CreateAlias("a")},
			TableExist:     true,
		},
	}
	rightScan.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	rightScan.ObjRef = &planpb.ObjectRef{ObjName: "right_t"}
	require.NoError(t, builder.recordIndexHints(rightScanID, rightScan.TableDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{PrimaryKeyName, "idx_a", "idx_b"},
	}}))

	newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, rightScanID, joinNode.Children[1])

	matchingAccessID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		Stats:    DefaultStats(),
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan: true,
			IndexName:   "idx_b",
		},
	}, builder.ctxByNode[joinID])
	builder.inheritIndexHints(matchingAccessID, rightScanID)
	joinNode.Children[1] = matchingAccessID

	newID, err = builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, matchingAccessID, joinNode.Children[1])

	matchingWrapperID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_INDEX,
		Children: []int32{rightScanID, matchingAccessID},
	}, builder.ctxByNode[joinID])
	joinNode.Children[1] = matchingWrapperID

	newID, err = builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, matchingWrapperID, joinNode.Children[1])
}

func TestForceIndexForJoinReplacesRealCoveringFilterScan(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	tableDef := mock.ctxt.tables["index_hint_t"]
	tableDef.Indexes = append([]*planpb.IndexDef{
		{IndexName: "idx_filter", IndexTableName: "idx_hint_filter", Parts: []string{"a", "b", catalog.CreateAlias("id")}, TableExist: true},
		{IndexName: "idx_join", IndexTableName: "idx_hint_join", Parts: []string{"b", "a", catalog.CreateAlias("id")}, TableExist: true},
	}, tableDef.Indexes...)
	addIndexHintIndexTableForTest(mock, "idx_hint_filter", 25363)
	addIndexHintIndexTableForTest(mock, "idx_hint_join", 25364)

	queryPlan, err := runOneStmt(mock, t, `
		select l.a, r.a, r.b
		from index_hint_t l
		join index_hint_t r force index for join(idx_join) on l.b = r.b
		where r.a = 1`)
	require.NoError(t, err)
	reachableIndexes := reachableIndexScanNames(queryPlan.GetQuery())
	require.Contains(t, reachableIndexes, "idx_join")
	require.NotContains(t, reachableIndexes, "idx_filter")
}

func reachableIndexScanNames(query *planpb.Query) []string {
	if query == nil {
		return nil
	}
	visited := make(map[int32]struct{})
	var names []string
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return
		}
		if _, ok := visited[nodeID]; ok {
			return
		}
		visited[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		if node == nil {
			return
		}
		if node.IndexScanInfo.IsIndexScan {
			names = append(names, node.IndexScanInfo.IndexName)
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	for _, rootID := range query.Steps {
		visit(rootID)
	}
	return names
}

func TestForcePrimaryForJoinPreservesHashOnPKDirection(t *testing.T) {
	builder, joinID, leftScanID, _ := makeIndexHintJoinBuilder(t)
	joinNode := builder.qry.Nodes[joinID]
	rightScanID := joinNode.Children[1]
	rightScan := builder.qry.Nodes[rightScanID]
	rightScan.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "b", Names: []string{"b"}}
	require.NoError(t, builder.recordIndexHints(rightScanID, rightScan.TableDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{PrimaryKeyName},
	}}))
	joinNode.Stats.HashmapStats.HashOnPK = true
	onList := DeepCopyExprList(joinNode.OnList)

	newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, []int32{leftScanID, rightScanID}, joinNode.Children)
	require.Equal(t, onList, joinNode.OnList)
	require.True(t, joinNode.Stats.HashmapStats.HashOnPK)
}

func TestForceIndexForJoinReturnsMetadataErrorsAtomically(t *testing.T) {
	for _, nilMetadata := range []bool{false, true} {
		builder, joinID, leftScanID, leftDef := makeIndexHintJoinBuilder(t)
		builder.compCtx = &indexHintResolveFailureContext{
			MockCompilerContext: NewMockCompilerContext(true), nilMetadata: nilMetadata,
		}
		require.NoError(t, builder.recordIndexHints(leftScanID, leftDef, []*tree.IndexHint{{
			HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_a"},
		}}))
		joinNode := builder.qry.Nodes[joinID]
		children := slices.Clone(joinNode.Children)
		onList := DeepCopyExprList(joinNode.OnList)
		nodeCount := len(builder.qry.Nodes)

		newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
		require.Error(t, err)
		require.Equal(t, int32(-1), newID)
		require.Equal(t, nodeCount, len(builder.qry.Nodes))
		require.Equal(t, children, joinNode.Children)
		require.Equal(t, onList, joinNode.OnList)
		require.Empty(t, joinNode.RuntimeFilterBuildList)
	}
}

func TestRightForceIndexForJoinRollsBackSwapOnMetadataError(t *testing.T) {
	builder, joinID, _, _ := makeIndexHintJoinBuilder(t)
	builder.compCtx = &indexHintResolveFailureContext{MockCompilerContext: NewMockCompilerContext(true)}
	joinNode := builder.qry.Nodes[joinID]
	rightScanID := joinNode.Children[1]
	rightScan := builder.qry.Nodes[rightScanID]
	rightScan.TableDef.Indexes = []*planpb.IndexDef{{
		IndexName: "idx_b", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
		IndexTableName: "idx_join_b_table", Parts: []string{"b", catalog.CreateAlias("a")}, TableExist: true,
	}}
	rightScan.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	rightScan.ObjRef = &planpb.ObjectRef{ObjName: "right_t"}
	require.NoError(t, builder.recordIndexHints(rightScanID, rightScan.TableDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_b"},
	}}))
	children := slices.Clone(joinNode.Children)
	onList := DeepCopyExprList(joinNode.OnList)

	newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.Error(t, err)
	require.Equal(t, int32(-1), newID)
	require.Equal(t, children, joinNode.Children)
	require.Equal(t, onList, joinNode.OnList)
}

func TestForceIndexForJoinAllowsForcingBothSides(t *testing.T) {
	builder, joinID, leftScanID, leftDef := makeIndexHintJoinBuilder(t)
	joinNode := builder.qry.Nodes[joinID]
	rightScanID := joinNode.Children[1]
	rightDef := builder.qry.Nodes[rightScanID].TableDef
	rightDef.Indexes = []*planpb.IndexDef{{
		IndexName: "idx_b", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
		IndexTableName: "idx_join_b_table", Parts: []string{"b", catalog.CreateAlias("a")}, TableExist: true,
	}}
	rightDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	builder.qry.Nodes[rightScanID].ObjRef = &planpb.ObjectRef{ObjName: "right_t"}
	require.NoError(t, builder.recordIndexHints(leftScanID, leftDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_a"},
	}}))
	require.NoError(t, builder.recordIndexHints(rightScanID, rightDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_b"},
	}}))

	newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.NotEqual(t, leftScanID, joinNode.Children[0])
	indexJoin := builder.qry.Nodes[joinNode.Children[0]]
	require.Equal(t, planpb.Node_INDEX, indexJoin.JoinType)
	require.Equal(t, "idx_a", builder.qry.Nodes[indexJoin.Children[1]].IndexScanInfo.IndexName)
	require.Equal(t, "idx_b", findIndexScanNameForTable(builder.qry, "right_t"))
}

func findIndexScanNameForTable(query *planpb.Query, table string) string {
	for _, node := range query.Nodes {
		if node.IndexScanInfo.IsIndexScan && node.IndexScanInfo.BelongToTable == table {
			return node.IndexScanInfo.IndexName
		}
	}
	return ""
}

func TestForceIndexForJoinDoesNotBlockOuterJoinFilterAccess(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	addOuterJoinHintTable := func(name, indexName string, tableID uint64) {
		tableDef := DeepCopyTableDef(mock.ctxt.tables["index_hint_t"], true)
		tableDef.Name = name
		tableDef.TblId = tableID
		tableDef.Indexes = []*planpb.IndexDef{{
			IndexName: indexName, IndexTableName: "idx_hint_a",
			Parts: []string{"a", catalog.CreateAlias("id")}, TableExist: true,
		}}
		mock.ctxt.objects[name] = &ObjectRef{SchemaName: "tpch", ObjName: name, Obj: int64(tableID)}
		mock.ctxt.tables[name] = tableDef
		mock.ctxt.id2name[tableID] = name
		mock.ctxt.pks[name] = []int{0}
	}
	addOuterJoinHintTable("left_hint_t", "idx_left_a", 25361)
	addOuterJoinHintTable("right_hint_t", "idx_right_a", 25362)

	queryPlan, err := runOneStmt(mock, t, `
		select l.a
		from left_hint_t l force index for join(idx_left_a)
		left join right_hint_t r on l.a = r.a
		where l.a = 1`)
	require.NoError(t, err)
	require.Equal(t, "idx_left_a", findIndexScanNameForTable(queryPlan.GetQuery(), "left_hint_t"))

	queryPlan, err = runOneStmt(mock, t, `
		select r.a
		from left_hint_t l
		right join right_hint_t r force index for join(idx_right_a) on l.a = r.a
		where r.a = 1`)
	require.NoError(t, err)
	require.Equal(t, "idx_right_a", findIndexScanNameForTable(queryPlan.GetQuery(), "right_hint_t"))
}

func TestForceIndexForJoinIsConsumedInsideThreeTableTree(t *testing.T) {
	builder, innerJoinID, _, _ := makeIndexHintJoinBuilder(t)
	innerJoin := builder.qry.Nodes[innerJoinID]
	targetScanID := innerJoin.Children[1]
	targetScan := builder.qry.Nodes[targetScanID]
	targetScan.TableDef.Indexes = []*planpb.IndexDef{{
		IndexName: "idx_b", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
		IndexTableName: "idx_join_b_table", Parts: []string{"b", catalog.CreateAlias("a")}, TableExist: true,
	}}
	targetScan.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	targetScan.ObjRef = &planpb.ObjectRef{ObjName: "target_t"}
	targetScan.Stats = &planpb.Stats{TableCnt: 1000, Cost: 1000}
	require.NoError(t, builder.recordIndexHints(targetScanID, targetScan.TableDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_b"},
	}}))

	ctx := builder.ctxByNode[innerJoinID]
	thirdTag := builder.genNewBindTag()
	thirdDef := &planpb.TableDef{
		Name: "third_t", Cols: []*planpb.ColDef{{Name: "a", Typ: planpb.Type{Id: int32(types.T_int32)}}},
		Name2ColIndex: map[string]int32{"a": 0},
	}
	thirdScanID := builder.appendNode(makeJoinIndexTestScan(thirdDef, thirdTag), ctx)
	outerCond := ftjMakeEqExpr(t, ftjColExpr(targetScan.TableDef, targetScan.BindingTags[0], 0), ftjColExpr(thirdDef, thirdTag, 0))
	outerJoinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{innerJoinID, thirdScanID}, OnList: []*planpb.Expr{outerCond},
	}, ctx)

	newID, err := builder.applyIndices(outerJoinID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, outerJoinID, newID)
	require.NotEqual(t, targetScanID, innerJoin.Children[1])
	require.Equal(t, planpb.Node_INDEX, builder.qry.Nodes[innerJoin.Children[1]].JoinType)
	require.Equal(t, targetScanID, builder.qry.Nodes[innerJoin.Children[1]].Children[0])
}

func TestForceIndexPrepassStopsAtQueryBlockBoundary(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	outerCtx := NewBindContext(builder, nil)
	innerCtx := NewBindContext(builder, outerCtx)
	scanTag := builder.genNewBindTag()
	projectTag := builder.genNewBindTag()
	typ := planpb.Type{Id: int32(types.T_int32)}
	tableDef := &planpb.TableDef{
		Name:          "t",
		Cols:          []*planpb.ColDef{{Name: "a", Typ: typ}},
		Name2ColIndex: map[string]int32{"a": 0},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
		Indexes: []*planpb.IndexDef{{
			IndexName: "idx_a", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			IndexTableName: "idx_a_table", Parts: []string{"a"}, TableExist: true,
		}},
	}
	scanID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{scanTag}, TableDef: tableDef,
		ObjRef: &planpb.ObjectRef{ObjName: "t"},
	}, innerCtx)
	require.NoError(t, builder.recordIndexHints(scanID, tableDef, []*tree.IndexHint{
		{HintType: tree.HintForce, HintScope: tree.HintForOrderBy, IndexNames: []string{"idx_a"}},
		{HintType: tree.HintForce, HintScope: tree.HintForGroupBy, IndexNames: []string{PrimaryKeyName}},
	}))
	aggID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_AGG, Children: []int32{scanID}, GroupBy: []*planpb.Expr{GetColExpr(typ, scanTag, 0)},
	}, innerCtx)
	projectID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{aggID}, BindingTags: []int32{projectTag},
		ProjectList: []*planpb.Expr{GetColExpr(typ, scanTag, 0)},
	}, outerCtx)
	sortID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_SORT, Children: []int32{projectID},
		OrderBy: []*planpb.OrderBySpec{{Expr: GetColExpr(typ, projectTag, 0)}},
	}, outerCtx)

	newID, err := builder.applyForceIndexHints(sortID, nil, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, sortID, newID)
	require.Equal(t, aggID, builder.qry.Nodes[projectID].Children[0])
	require.Equal(t, scanID, builder.qry.Nodes[aggID].Children[0])
	require.True(t, builder.isScanProtected(scanID))
	require.Empty(t, findFirstIndexScanName(&planpb.Plan{Plan: &planpb.Plan_Query{Query: builder.qry}}))
	require.Len(t, builder.qry.Nodes, 4)
}

func makeIndexHintJoinBuilder(t *testing.T) (*QueryBuilder, int32, int32, *planpb.TableDef) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)
	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()
	leftDef := makeJoinHintTestTableDef()
	rightDef := &planpb.TableDef{
		Name: "right_t",
		Cols: []*planpb.ColDef{
			{Name: "a", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "b", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
		Name2ColIndex: map[string]int32{"a": 0, "b": 1},
	}
	leftScanID := builder.appendNode(makeJoinIndexTestScan(leftDef, leftTag), ctx)
	rightScanID := builder.appendNode(makeJoinIndexTestScan(rightDef, rightTag), ctx)
	builder.qry.Nodes[rightScanID].Stats.Outcnt = 0
	builder.qry.Nodes[rightScanID].Stats.Selectivity = 0.01
	registerFullTextJoinRegularIndexTable(builder, "idx_join_a_table")
	registerFullTextJoinRegularIndexTable(builder, "idx_join_b_table")
	joinCond := ftjMakeEqExpr(t, ftjColExpr(leftDef, leftTag, 1), ftjColExpr(rightDef, rightTag, 1))
	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		Children: []int32{leftScanID, rightScanID},
		JoinType: planpb.Node_INNER,
		OnList:   []*planpb.Expr{joinCond},
	}, ctx)
	return builder, joinID, leftScanID, leftDef
}

func TestIndexHintOrderScopeControlsTopSortRewrite(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	tag := builder.genNewBindTag()
	indexScan := &planpb.Node{
		NodeId:      1,
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		TableDef: &planpb.TableDef{
			Name: "idx_table",
			Cols: []*planpb.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
				{Name: catalog.IndexTablePrimaryColName, Typ: planpb.Type{Id: int32(types.T_int32)}},
			},
		},
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan: true,
			IndexName:   "idx_a",
			Parts:       []string{"a", catalog.CreateAlias("id")},
		},
	}
	require.True(t, builder.regularIndexScanAllowedByOrderHints(indexScan))
	builder.indexHintsByScan = map[int32]*indexHintSet{
		indexScan.NodeId: {
			order: indexHintScopeSet{ignore: map[string]struct{}{"idx_a": {}}},
		},
	}
	require.False(t, builder.regularIndexScanAllowedByOrderHints(indexScan))
}

func makeJoinHintTestTableDef() *planpb.TableDef {
	return &planpb.TableDef{
		Name: "left_t",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "a", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "b", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
		Name2ColIndex: map[string]int32{
			"id": 0,
			"a":  1,
			"b":  2,
		},
		Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Indexes: []*planpb.IndexDef{
			{
				IndexName:      "idx_a",
				IndexTableName: "idx_join_a_table",
				Parts:          []string{"a", "id"},
				TableExist:     true,
			},
			{
				IndexName:      "idx_b",
				IndexTableName: "idx_join_b_table",
				Parts:          []string{"b", "id"},
				TableExist:     true,
			},
		},
	}
}

func addIndexHintChoiceTableForTest(mock *MockOptimizer) {
	idType := planpb.Type{Id: int32(types.T_int32), NotNullable: true}
	intType := planpb.Type{Id: int32(types.T_int32)}
	rowIDType := planpb.Type{Id: int32(types.T_Rowid), NotNullable: true, Width: 16}
	mainTable := &planpb.TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     25356,
		Name:      "index_hint_t",
		Cols: []*planpb.ColDef{
			{ColId: 0, Name: "id", OriginName: "id", Typ: idType, Primary: true, Pkidx: 1, Default: &planpb.Default{}},
			{ColId: 1, Name: "a", OriginName: "a", Typ: intType, Default: &planpb.Default{NullAbility: true}},
			{ColId: 2, Name: "b", OriginName: "b", Typ: intType, Default: &planpb.Default{NullAbility: true}},
			{ColId: 3, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &planpb.Default{}},
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Cols:        []uint64{0},
			Names:       []string{"id"},
		},
		Indexes: []*planpb.IndexDef{
			{
				IndexName:      "idx_a",
				Parts:          []string{"a", catalog.CreateAlias("id")},
				IndexTableName: "idx_hint_a",
				TableExist:     true,
			},
			{
				IndexName:      "idx_ab",
				Parts:          []string{"a", "b", catalog.CreateAlias("id")},
				IndexTableName: "idx_hint_ab",
				TableExist:     true,
			},
			{
				IndexName:      "uk_ab",
				Parts:          []string{"a", "b"},
				IndexTableName: "uk_hint_ab",
				TableExist:     true,
				Unique:         true,
			},
			{
				IndexName:      "idx_id",
				Parts:          []string{"id", catalog.CreateAlias("id")},
				IndexTableName: "idx_hint_id",
				TableExist:     true,
			},
		},
		Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2},
	}

	mock.ctxt.objects["index_hint_t"] = &ObjectRef{SchemaName: "tpch", ObjName: "index_hint_t", Obj: 25356}
	mock.ctxt.tables["index_hint_t"] = mainTable
	mock.ctxt.id2name[25356] = "index_hint_t"
	mock.ctxt.pks["index_hint_t"] = []int{0}
	addIndexHintIndexTableForTest(mock, "idx_hint_a", 25357)
	addIndexHintIndexTableForTest(mock, "idx_hint_ab", 25358)
	addIndexHintIndexTableForTest(mock, "uk_hint_ab", 25359)
	addIndexHintIndexTableForTest(mock, "idx_hint_id", 25360)
}

func addGroupedJoinIndexTablesForTest(mock *MockOptimizer) {
	intType := planpb.Type{Id: int32(types.T_int32), NotNullable: true}
	bigintType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	stateType := planpb.Type{Id: int32(types.T_varchar), Width: 12, NotNullable: true}
	smallintType := planpb.Type{Id: int32(types.T_int16), NotNullable: true}
	decimalType := planpb.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 3, NotNullable: true}
	compositeType := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, NotNullable: true}
	rowIDType := planpb.Type{Id: int32(types.T_Rowid), Width: 16, NotNullable: true}

	parent := &planpb.TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     2680201,
		Name:      "grouped_join_parent",
		Cols: []*planpb.ColDef{
			{ColId: 0, Name: "tenant_id", OriginName: "tenant_id", Typ: intType, Primary: true, Pkidx: 1, Default: &planpb.Default{}},
			{ColId: 1, Name: "id", OriginName: "id", Typ: bigintType, Primary: true, Pkidx: 2, Default: &planpb.Default{}},
			{ColId: 2, Name: "state", OriginName: "state", Typ: stateType, Default: &planpb.Default{}},
			{ColId: 3, Name: catalog.CPrimaryKeyColName, OriginName: catalog.CPrimaryKeyColName, Typ: compositeType, Hidden: true, Default: &planpb.Default{}},
			{ColId: 4, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &planpb.Default{}},
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Cols:        []uint64{0, 1},
			Names:       []string{"tenant_id", "id"},
			CompPkeyCol: &planpb.ColDef{Name: catalog.CPrimaryKeyColName, Typ: compositeType, Hidden: true},
		},
		Indexes: []*planpb.IndexDef{{
			IndexName:      "idx_tenant_state_id",
			Parts:          []string{"tenant_id", "state", "id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
			IndexTableName: "grouped_join_parent_idx",
			TableExist:     true,
		}},
		Name2ColIndex: map[string]int32{
			"tenant_id":                0,
			"id":                       1,
			"state":                    2,
			catalog.CPrimaryKeyColName: 3,
			catalog.Row_ID:             4,
		},
	}
	child := &planpb.TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     2680202,
		Name:      "grouped_join_child",
		Cols: []*planpb.ColDef{
			{ColId: 0, Name: "tenant_id", OriginName: "tenant_id", Typ: intType, Primary: true, Pkidx: 1, Default: &planpb.Default{}},
			{ColId: 1, Name: "id", OriginName: "id", Typ: bigintType, Primary: true, Pkidx: 2, Default: &planpb.Default{}},
			{ColId: 2, Name: "tag_id", OriginName: "tag_id", Typ: smallintType, Primary: true, Pkidx: 3, Default: &planpb.Default{}},
			{ColId: 3, Name: "weight", OriginName: "weight", Typ: decimalType, Default: &planpb.Default{}},
			{ColId: 4, Name: catalog.CPrimaryKeyColName, OriginName: catalog.CPrimaryKeyColName, Typ: compositeType, Hidden: true, Default: &planpb.Default{}},
			{ColId: 5, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &planpb.Default{}},
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Cols:        []uint64{0, 1, 2},
			Names:       []string{"tenant_id", "id", "tag_id"},
			CompPkeyCol: &planpb.ColDef{Name: catalog.CPrimaryKeyColName, Typ: compositeType, Hidden: true},
		},
		Name2ColIndex: map[string]int32{
			"tenant_id":                0,
			"id":                       1,
			"tag_id":                   2,
			"weight":                   3,
			catalog.CPrimaryKeyColName: 4,
			catalog.Row_ID:             5,
		},
	}
	indexTable := &planpb.TableDef{
		TableType: catalog.SystemIndexRel,
		TblId:     2680203,
		Name:      "grouped_join_parent_idx",
		Cols: []*planpb.ColDef{
			{ColId: 0, Name: catalog.IndexTableIndexColName, OriginName: catalog.IndexTableIndexColName, Typ: compositeType, Primary: true, Default: &planpb.Default{}},
			{ColId: 1, Name: catalog.IndexTablePrimaryColName, OriginName: catalog.IndexTablePrimaryColName, Typ: compositeType, Default: &planpb.Default{}},
			{ColId: 2, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &planpb.Default{}},
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: catalog.IndexTableIndexColName,
			Cols:        []uint64{0},
			Names:       []string{catalog.IndexTableIndexColName},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
			catalog.Row_ID:                   2,
		},
	}

	for _, tableDef := range []*planpb.TableDef{parent, child, indexTable} {
		mock.ctxt.objects[tableDef.Name] = &ObjectRef{SchemaName: "tpch", ObjName: tableDef.Name, Obj: int64(tableDef.TblId)}
		mock.ctxt.tables[tableDef.Name] = tableDef
		mock.ctxt.id2name[tableDef.TblId] = tableDef.Name
	}
	mock.ctxt.pks[parent.Name] = []int{0, 1}
	mock.ctxt.pks[child.Name] = []int{0, 1, 2}
	mock.ctxt.pks[indexTable.Name] = []int{0}
}

func addGroupedJoinAlternativeIndexForTest(mock *MockOptimizer) {
	parent := mock.ctxt.tables["grouped_join_parent"]
	parent.Indexes = append([]*planpb.IndexDef{{
		IndexName:      "idx_state",
		Parts:          []string{"state", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		IndexTableName: "grouped_join_parent_idx_state",
		TableExist:     true,
	}}, parent.Indexes...)

	indexTable := DeepCopyTableDef(mock.ctxt.tables["grouped_join_parent_idx"], true)
	indexTable.TblId = 2680204
	indexTable.Name = "grouped_join_parent_idx_state"
	mock.ctxt.objects[indexTable.Name] = &ObjectRef{
		SchemaName: "tpch", ObjName: indexTable.Name, Obj: int64(indexTable.TblId),
	}
	mock.ctxt.tables[indexTable.Name] = indexTable
	mock.ctxt.id2name[indexTable.TblId] = indexTable.Name
	mock.ctxt.pks[indexTable.Name] = []int{0}
}

func addIndexHintPayloadColumnForTest(mock *MockOptimizer) {
	tableDef := mock.ctxt.tables["index_hint_t"]
	payloadPos := int32(len(tableDef.Cols))
	tableDef.Cols = append(tableDef.Cols, &planpb.ColDef{
		ColId: 4, Name: "payload", OriginName: "payload",
		Typ:     planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Default: &planpb.Default{NullAbility: true},
	})
	tableDef.Name2ColIndex["payload"] = payloadPos
}

func addIndexHintIndexTableForTest(mock *MockOptimizer, name string, tableID uint64) {
	keyType := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	pkType := planpb.Type{Id: int32(types.T_int32), NotNullable: true}
	rowIDType := planpb.Type{Id: int32(types.T_Rowid), NotNullable: true, Width: 16}
	tableDef := &planpb.TableDef{
		TableType: catalog.SystemIndexRel,
		TblId:     tableID,
		Name:      name,
		Cols: []*planpb.ColDef{
			{ColId: 0, Name: catalog.IndexTableIndexColName, OriginName: catalog.IndexTableIndexColName, Typ: keyType, Primary: true, Default: &planpb.Default{}},
			{ColId: 1, Name: catalog.IndexTablePrimaryColName, OriginName: catalog.IndexTablePrimaryColName, Typ: pkType, Default: &planpb.Default{}},
			{ColId: 2, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &planpb.Default{}},
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: catalog.IndexTableIndexColName,
			Cols:        []uint64{0},
			Names:       []string{catalog.IndexTableIndexColName},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
		},
	}
	mock.ctxt.objects[name] = &ObjectRef{SchemaName: "tpch", ObjName: name, Obj: int64(tableID)}
	mock.ctxt.tables[name] = tableDef
	mock.ctxt.id2name[tableID] = name
	mock.ctxt.pks[name] = []int{0}
}

func findFirstIndexScanName(p *Plan) string {
	node := findFirstIndexScanNode(p)
	if node == nil {
		return ""
	}
	return node.IndexScanInfo.IndexName
}

func findFirstIndexScanNode(p *Plan) *planpb.Node {
	if p == nil || p.GetQuery() == nil {
		return nil
	}
	for _, node := range p.GetQuery().Nodes {
		if node.IndexScanInfo.IsIndexScan {
			return node
		}
	}
	return nil
}

func planHasIndexJoin(p *Plan) bool {
	if p == nil || p.GetQuery() == nil {
		return false
	}
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_INDEX {
			return true
		}
	}
	return false
}

func planHasSort(p *Plan) bool {
	if p == nil || p.GetQuery() == nil {
		return false
	}
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == planpb.Node_SORT {
			return true
		}
	}
	return false
}

func TestTryIndexOnlyScan_RandomRangesNotRejected(t *testing.T) {
	// Exercise the pure candidate matcher with a node that simulates sysbench
	// random_ranges (10M rows, outcnt=10000, selectivity=0.001).
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)

	idxDef := &IndexDef{
		IndexName:      "idx_k",
		Parts:          []string{"k", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "idx_tbl_k",
		TableExist:     true,
	}

	kColPos := int32(1)
	bindTag := builder.genNewBindTag()

	makeNode := func(tableCnt, outcnt, selectivity float64) *planpb.Node {
		return &planpb.Node{
			BindingTags: []int32{bindTag},
			TableDef: &planpb.TableDef{
				Name: "sbtest1",
				Name2ColIndex: map[string]int32{
					"k":                           kColPos,
					catalog.FakePrimaryKeyColName: 0,
				},
				Cols: []*planpb.ColDef{
					{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
					{Name: "k", Typ: planpb.Type{Id: int32(types.T_int32)}},
				},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: catalog.FakePrimaryKeyColName,
				},
				Indexes: []*planpb.IndexDef{idxDef},
			},
			Stats: &planpb.Stats{
				TableCnt:    tableCnt,
				Outcnt:      outcnt,
				Selectivity: selectivity,
			},
			FilterList: []*planpb.Expr{
				{
					Expr: &planpb.Expr_F{
						F: &planpb.Function{
							Func: &ObjectRef{ObjName: "between"},
							Args: []*planpb.Expr{
								{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: bindTag, ColPos: kColPos}}},
								{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 100}}}},
								{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 200}}}},
							},
						},
					},
				},
			},
		}
	}

	colRefCnt := map[[2]int32]int{
		{bindTag, kColPos}: 1,
	}

	candidateMatches := func(node *planpb.Node) bool {
		costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
		_, ok := builder.matchRegularIndexOnlyScan(idxDef, node, costCtx)
		return ok
	}

	// random_ranges scenario: 10M rows, outcnt=10000, selectivity=0.001
	// With the fix (PK card limit = 1M), guard should PASS.
	assert.True(t, candidateMatches(makeNode(10_000_000, 10_000, 0.001)),
		"random_ranges (10M rows, outcnt=10000) should pass the guard")

	// Same but with outcnt=50000 (still well below 1M threshold)
	assert.True(t, candidateMatches(makeNode(10_000_000, 50_000, 0.005)),
		"moderate outcnt (50000) on 10M table should pass the guard")

	// High outcnt (2M on 10M table) → guard should REJECT
	assert.False(t, candidateMatches(makeNode(10_000_000, 2_000_000, 0.2)),
		"high outcnt (2M) should be rejected by guard")

	// High selectivity (0.5) → guard should REJECT
	assert.False(t, candidateMatches(makeNode(10_000_000, 5_000_000, 0.5)),
		"high selectivity (0.5) should be rejected by guard")

	// Verify threshold arithmetic
	assert.True(t, 10_000 >= InFilterCardLimitNonPK,
		"random_ranges outcnt (10000) >= old threshold (10000) → old code would reject")
	assert.True(t, float64(10_000) < float64(GetInFilterCardLimitOnPK("", 10_000_000)),
		"random_ranges outcnt (10000) < new threshold (1M) → new code allows")
}

func TestTryIndexOnlyScanRejectsBroadEncodedEquality(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	bindTag := builder.genNewBindTag()
	idxDef := &planpb.IndexDef{
		IndexName:      "idx_category_tenant_time_id",
		IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
		IndexTableName: "__mo_idx_category_tenant_time_id",
		Parts: []string{
			"category",
			"tenant_id",
			"event_time",
			"event_id",
			catalog.CreateAlias(catalog.CPrimaryKeyColName),
		},
		TableExist: true,
	}
	registerMockIndexTable(t, builder, idxDef.IndexTableName)
	node := &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "events"},
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name: "events",
			Cols: []*planpb.ColDef{
				{Name: "tenant_id", Typ: planpb.Type{Id: int32(types.T_int32)}},
				{Name: "event_id", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "category", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 12}},
				{Name: "event_time", Typ: planpb.Type{Id: int32(types.T_datetime)}},
				{Name: catalog.CPrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, Hidden: true},
			},
			Name2ColIndex: map[string]int32{
				"tenant_id":                0,
				"event_id":                 1,
				"category":                 2,
				"event_time":               3,
				catalog.CPrimaryKeyColName: 4,
			},
			Pkey: &planpb.PrimaryKeyDef{
				PkeyColName: catalog.CPrimaryKeyColName,
				Names:       []string{"tenant_id", "event_id"},
			},
		},
		Stats: &planpb.Stats{
			TableCnt:    800_000,
			Outcnt:      720_000,
			Selectivity: 0.9,
			Cost:        800_000,
		},
		FilterList: []*planpb.Expr{makeStringEqFilterExpr(bindTag, 2, "HOT")},
	}
	node.FilterList[0].Selectivity = 0.9
	scanID := builder.appendNode(node, ctx)
	builder.qry.Nodes[scanID].Stats = &planpb.Stats{TableCnt: 800_000, Outcnt: 720_000, Selectivity: 0.9, Cost: 800_000}
	builder.qry.Nodes[scanID].FilterList[0].Selectivity = 0.9
	leadingPos, _ := findLeadingFilter(idxDef, builder.qry.Nodes[scanID])
	require.True(t, builder.shouldSkipEncodedIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], map[[2]int32]int{{bindTag, 1}: 1, {bindTag, 2}: 1}, leadingPos, false))

	idxNodeID := builder.tryIndexOnlyScan(
		idxDef,
		builder.qry.Nodes[scanID],
		map[[2]int32]int{{bindTag, 1}: 1, {bindTag, 2}: 1},
		map[[2]int32]*planpb.Expr{},
		&Snapshot{},
	)

	require.Equal(t, int32(-1), idxNodeID)
}

type encodedIndexCostTestContext struct {
	*MockCompilerContext
	statsCache *StatsCache
}

type encodedIndexPlanTestContext struct {
	*MockCompilerContext
	statsCache *StatsCache
	statsByID  map[uint64]*statspb.StatsInfo
}

func (ctx *encodedIndexPlanTestContext) GetStatsCache() *StatsCache {
	return ctx.statsCache
}

func (ctx *encodedIndexPlanTestContext) Stats(obj *planpb.ObjectRef, _ *Snapshot) (*statspb.StatsInfo, error) {
	if obj == nil {
		return nil, nil
	}
	return ctx.statsByID[uint64(obj.Obj)], nil
}

type encodedIndexPlanTestOptimizer struct {
	ctx *encodedIndexPlanTestContext
}

const encodedIndexCostUnitTableID uint64 = 10_001

func (optimizer *encodedIndexPlanTestOptimizer) CurrentContext() CompilerContext {
	return optimizer.ctx
}

func (optimizer *encodedIndexPlanTestOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	queryPlan, err := BuildPlan(optimizer.ctx, stmt, false)
	if err != nil {
		return nil, err
	}
	return queryPlan.GetQuery(), nil
}

func (ctx *encodedIndexCostTestContext) GetStatsCache() *StatsCache {
	return ctx.statsCache
}

func newEncodedIndexCostTestCase(
	t testing.TB,
	parts []string,
	filters []*planpb.Expr,
	stats *planpb.Stats,
	colRefs map[int32]int,
	force bool,
) (*QueryBuilder, int32, *planpb.IndexDef, map[[2]int32]int) {
	t.Helper()

	baseCtx := NewMockCompilerContext(true)
	statsCache := NewStatsCache()
	statsInfo := NewStatsInfo()
	statsInfo.TableCnt = 800_000
	statsInfo.SizeMap = map[string]uint64{
		"tenant_id":                800_000 * 4,
		"event_id":                 800_000 * 8,
		"category":                 800_000 * 4,
		"event_time":               800_000 * 8,
		catalog.CPrimaryKeyColName: 800_000 * 14,
	}
	statsCache.Set(encodedIndexCostUnitTableID, statsInfo)
	testCtx := &encodedIndexCostTestContext{MockCompilerContext: baseCtx, statsCache: statsCache}
	builder := NewQueryBuilder(planpb.Query_SELECT, testCtx, false, true)
	ctx := NewBindContext(builder, nil)
	bindTag := builder.genNewBindTag()
	idxDef := &planpb.IndexDef{
		IndexName:      "idx_cost_test",
		IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
		IndexTableName: "__mo_idx_cost_test",
		Parts:          parts,
		TableExist:     true,
	}
	indexTableDef := makeTestIndexTableDef()
	indexTableDef.Name = idxDef.IndexTableName
	key := strings.ToLower(idxDef.IndexTableName)
	testCtx.objects[key] = &planpb.ObjectRef{SchemaName: "test", ObjName: idxDef.IndexTableName}
	testCtx.tables[key] = indexTableDef

	node := &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "events", Obj: int64(encodedIndexCostUnitTableID)},
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			TblId: encodedIndexCostUnitTableID,
			Name:  "events",
			Cols: []*planpb.ColDef{
				{Name: "tenant_id", Typ: planpb.Type{Id: int32(types.T_int32)}},
				{Name: "event_id", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "category", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 12}},
				{Name: "event_time", Typ: planpb.Type{Id: int32(types.T_datetime)}},
				{Name: catalog.CPrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, Hidden: true},
			},
			Name2ColIndex: map[string]int32{
				"tenant_id":                0,
				"event_id":                 1,
				"category":                 2,
				"event_time":               3,
				catalog.CPrimaryKeyColName: 4,
			},
			Pkey: &planpb.PrimaryKeyDef{
				PkeyColName: catalog.CPrimaryKeyColName,
				Names:       []string{"tenant_id", "event_id"},
			},
		},
		FilterList: filters,
	}
	scanID := builder.appendNode(node, ctx)
	builder.qry.Nodes[scanID].Stats = stats

	refCnt := make(map[[2]int32]int, len(colRefs))
	for colPos, count := range colRefs {
		refCnt[[2]int32{bindTag, colPos}] = count
	}
	if force {
		if builder.indexHintsByScan == nil {
			builder.indexHintsByScan = make(map[int32]*indexHintSet)
		}
		builder.indexHintsByScan[scanID] = &indexHintSet{scan: indexHintScopeSet{
			forceSpecified: true,
			force:          map[string]struct{}{idxDef.IndexName: {}},
		}}
	}
	return builder, scanID, idxDef, refCnt
}

func TestTryIndexOnlyScanEncodedCostControls(t *testing.T) {
	wideParts := []string{
		"category",
		"tenant_id",
		"event_time",
		"event_id",
		catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}
	narrowParts := []string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
	tests := []struct {
		name       string
		parts      []string
		leadingSel float64
		outcnt     float64
		cost       float64
		colRefs    map[int32]int
		force      bool
		wantIndex  bool
	}{
		{
			name:  "selective encoded projection wins",
			parts: wideParts, leadingSel: 0.001, outcnt: 800, cost: 800_000,
			colRefs: map[int32]int{1: 1, 2: 1}, wantIndex: true,
		},
		{
			name:  "broad encoded projection loses",
			parts: wideParts, leadingSel: 0.9, outcnt: 720_000, cost: 800_000,
			colRefs: map[int32]int{1: 1, 2: 1},
		},
		{
			name:  "narrow key stays below crossover",
			parts: narrowParts, leadingSel: 0.10, outcnt: 80_000, cost: 800_000,
			colRefs: map[int32]int{2: 1}, wantIndex: true,
		},
		{
			name:  "wide key crosses at same selectivity",
			parts: wideParts, leadingSel: 0.10, outcnt: 80_000, cost: 800_000,
			colRefs: map[int32]int{2: 1},
		},
		{
			name:  "one extraction consumer stays below crossover",
			parts: wideParts, leadingSel: 0.05, outcnt: 40_000, cost: 800_000,
			colRefs: map[int32]int{1: 1, 2: 1}, wantIndex: true,
		},
		{
			name:  "repeated extraction consumers cross",
			parts: wideParts, leadingSel: 0.05, outcnt: 40_000, cost: 800_000,
			colRefs: map[int32]int{1: 5, 2: 1},
		},
		{
			name:  "small output limits downstream extraction",
			parts: wideParts, leadingSel: 0.1, outcnt: 800, cost: 800_000,
			colRefs: map[int32]int{1: 1, 2: 1}, wantIndex: true,
		},
		{
			name:  "large output charges downstream extraction",
			parts: wideParts, leadingSel: 0.1, outcnt: 120_000, cost: 800_000,
			colRefs: map[int32]int{1: 1, 2: 1},
		},
		{
			name:  "force index preserves compatibility escape",
			parts: wideParts, leadingSel: 0.9, outcnt: 720_000, cost: 800_000,
			colRefs: map[int32]int{1: 1, 2: 1}, force: true, wantIndex: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter := makeStringEqFilterExpr(0, 2, "category")
			filter.Selectivity = test.leadingSel
			builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
				t,
				test.parts,
				[]*planpb.Expr{filter},
				&planpb.Stats{TableCnt: 800_000, Outcnt: test.outcnt, Selectivity: test.outcnt / 800_000, Cost: test.cost},
				test.colRefs,
				test.force,
			)
			// The fixture allocates the first binding tag.
			builder.qry.Nodes[scanID].FilterList[0].GetF().Args[0].GetCol().RelPos = builder.qry.Nodes[scanID].BindingTags[0]

			idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{})
			if test.wantIndex {
				require.NotEqual(t, int32(-1), idxNodeID)
			} else {
				require.Equal(t, int32(-1), idxNodeID)
			}
		})
	}
}

func TestTryIndexOnlyScanUsesNullRejectingComparisonBounds(t *testing.T) {
	parts := []string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
	tests := []struct {
		name             string
		makeLiteral      func(int32) *planpb.Expr
		makeRuntime      func(int32) *planpb.Expr
		runtimeWantIndex bool
	}{
		{
			name:             "equality",
			runtimeWantIndex: true,
			makeLiteral: func(relPos int32) *planpb.Expr {
				return makeRangeFilterExpr(relPos, 0, "=", 7)
			},
			makeRuntime: func(relPos int32) *planpb.Expr {
				return makeParamEqFilterExpr(relPos, 0, 0)
			},
		},
		{
			name:             "in",
			runtimeWantIndex: true,
			makeLiteral: func(relPos int32) *planpb.Expr {
				return makeIntInFilterExpr(relPos, 0, 7, 8)
			},
			makeRuntime: func(relPos int32) *planpb.Expr {
				return makeParamInFilterExpr(relPos, 0, 2)
			},
		},
		{
			name:             "between",
			runtimeWantIndex: true,
			makeLiteral: func(relPos int32) *planpb.Expr {
				return makeIntBetweenFilterExpr(relPos, 0, 7, 8)
			},
			makeRuntime: func(relPos int32) *planpb.Expr {
				return makeParamBetweenFilterExpr(relPos, 0, 0, 1)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, variant := range []struct {
				name         string
				makeFilter   func(int32) *planpb.Expr
				wantResidual bool
				wantIndex    bool
			}{
				{name: "non-null literal stays below crossover", makeFilter: test.makeLiteral, wantIndex: true},
				{
					name: "runtime bounds preserve NULL semantics in the lookup", makeFilter: test.makeRuntime,
					wantIndex: test.runtimeWantIndex,
				},
			} {
				t.Run(variant.name, func(t *testing.T) {
					filter := variant.makeFilter(0)
					filter.Selectivity = 0.13
					builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
						t,
						parts,
						[]*planpb.Expr{filter},
						&planpb.Stats{TableCnt: 800_000, Outcnt: 104_000, Selectivity: 0.13, Cost: 800_000},
						map[int32]int{0: 1},
						false,
					)
					node := builder.qry.Nodes[scanID]
					node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
					costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
					match, ok := builder.matchRegularIndexOnlyScan(idxDef, node, costCtx)
					require.True(t, ok)
					require.Equal(t, variant.wantResidual, len(match.residualLeadingPos) > 0)

					idxNodeID := builder.tryIndexOnlyScan(idxDef, node, colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{})
					if variant.wantIndex {
						require.NotEqual(t, int32(-1), idxNodeID)
					} else {
						require.Equal(t, int32(-1), idxNodeID)
					}
				})
			}
		})
	}
}

func TestEncodedIndexCostChargesOnlyMaterializedLeadingResiduals(t *testing.T) {
	tenant := makeRangeFilterExpr(0, 0, "=", 7)
	tenant.Selectivity = 0.1
	category := makeStringEqFilterExpr(0, 2, "READY")
	category.Selectivity = 0.2
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		[]string{"tenant_id", "category", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{tenant, category},
		&planpb.Stats{TableCnt: 800_000, Outcnt: 16_000, Selectivity: 0.02, Cost: 800_000},
		map[int32]int{0: 1, 2: 1},
		false,
	)
	node := builder.qry.Nodes[scanID]
	for _, filter := range node.FilterList {
		filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	}

	costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
	match, ok := builder.matchRegularIndexOnlyScan(idxDef, node, costCtx)
	require.True(t, ok)
	require.Equal(t, []int32{1}, match.residualLeadingPos,
		"only the trailing byte-string prefix component needs an exact recheck")

	minimalWork, _, minimalValid := costCtx.score(
		idxDef, match.filterIdx, match.residualLeadingPos, encodedRegularIndexCostIndexOnly,
	)
	allLeadingWork, _, allLeadingValid := costCtx.score(
		idxDef, match.filterIdx, match.filterIdx, encodedRegularIndexCostIndexOnly,
	)
	require.True(t, minimalValid)
	require.True(t, allLeadingValid)
	require.Less(t, minimalWork, allLeadingWork,
		"candidate costing must not charge residual extraction that the plan does not materialize")
}

func TestRegularIndexCandidateRejectsEmptyParts(t *testing.T) {
	filter := makeStringEqFilterExpr(0, 2, "READY")
	filter.Selectivity = 0.01
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		nil,
		[]*planpb.Expr{filter},
		&planpb.Stats{TableCnt: 800_000, Outcnt: 8_000, Selectivity: 0.01, Cost: 800_000},
		map[int32]int{2: 1},
		false,
	)
	node := builder.qry.Nodes[scanID]
	node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	node.TableDef.Indexes = []*planpb.IndexDef{idxDef}

	require.NotPanics(t, func() {
		resultID := builder.applyIndicesForFiltersRegularIndex(
			scanID, node, colRefCnt, map[[2]int32]*planpb.Expr{},
		)
		require.Equal(t, scanID, resultID)
	})
}

func TestEncodedIndexCostBoundaryControls(t *testing.T) {
	wideParts := []string{
		"category", "tenant_id", "event_time", "event_id",
		catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}
	newBroadCase := func(t testing.TB, tableCnt float64) (*QueryBuilder, int32, *planpb.IndexDef, map[[2]int32]int) {
		filter := makeStringEqFilterExpr(0, 2, "HOT")
		filter.Selectivity = 0.9
		builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
			t, wideParts, []*planpb.Expr{filter},
			&planpb.Stats{TableCnt: tableCnt, Outcnt: tableCnt * 0.9, Selectivity: 0.9, Cost: tableCnt},
			map[int32]int{1: 1, 2: 1}, false,
		)
		node := builder.qry.Nodes[scanID]
		node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		if builder.indexHintsByScan == nil {
			builder.indexHintsByScan = make(map[int32]*indexHintSet)
		}
		return builder, scanID, idxDef, colRefCnt
	}

	t.Run("missing SizeMap uses type fallback", func(t *testing.T) {
		builder, scanID, idxDef, colRefCnt := newBroadCase(t, 800_000)
		ctx := builder.compCtx.(*encodedIndexCostTestContext)
		stats := NewStatsInfo()
		stats.TableCnt = 800_000
		ctx.statsCache.Set(encodedIndexCostUnitTableID, stats)
		require.Equal(t, int32(-1), builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{}))
	})

	t.Run("table threshold", func(t *testing.T) {
		for _, test := range []struct {
			name      string
			tableCnt  float64
			wantIndex bool
		}{
			{name: "49999 bypasses", tableCnt: 49_999, wantIndex: true},
			{name: "50000 costs", tableCnt: 50_000},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder, scanID, idxDef, colRefCnt := newBroadCase(t, test.tableCnt)
				idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{})
				if test.wantIndex {
					require.NotEqual(t, int32(-1), idxNodeID)
				} else {
					require.Equal(t, int32(-1), idxNodeID)
				}
			})
		}
	})

	t.Run("invalid and extreme statistics fail open", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			mutate func(*planpb.Node)
		}{
			{name: "NaN table count", mutate: func(node *planpb.Node) { node.Stats.TableCnt = math.NaN() }},
			{name: "positive infinity output", mutate: func(node *planpb.Node) { node.Stats.Outcnt = math.Inf(1) }},
			{name: "negative infinity cost", mutate: func(node *planpb.Node) { node.Stats.Cost = math.Inf(-1) }},
			{name: "NaN leading selectivity", mutate: func(node *planpb.Node) { node.FilterList[0].Selectivity = math.NaN() }},
			{name: "infinite leading selectivity", mutate: func(node *planpb.Node) { node.FilterList[0].Selectivity = math.Inf(1) }},
			{name: "extreme finite overflow", mutate: func(node *planpb.Node) {
				node.Stats.TableCnt, node.Stats.Outcnt, node.Stats.Cost = math.MaxFloat64, math.MaxFloat64, math.MaxFloat64
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder, scanID, idxDef, colRefCnt := newBroadCase(t, 800_000)
				node := builder.qry.Nodes[scanID]
				test.mutate(node)
				leadingPos, _ := findLeadingFilter(idxDef, node)
				require.False(t, builder.shouldSkipEncodedIndexOnlyScan(idxDef, node, colRefCnt, leadingPos, false))
			})
		}
	})

	t.Run("extreme SizeMap remains finite and rejects broad index", func(t *testing.T) {
		builder, scanID, idxDef, colRefCnt := newBroadCase(t, 800_000)
		ctx := builder.compCtx.(*encodedIndexCostTestContext)
		stats := NewStatsInfo()
		stats.TableCnt = 800_000
		stats.SizeMap = map[string]uint64{
			"tenant_id": math.MaxUint64, "event_id": math.MaxUint64,
			"category": math.MaxUint64, "event_time": math.MaxUint64,
			catalog.CPrimaryKeyColName: math.MaxUint64,
		}
		ctx.statsCache.Set(encodedIndexCostUnitTableID, stats)
		require.Equal(t, int32(-1), builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{}))
	})

	t.Run("equal work rejects automatic index", func(t *testing.T) {
		parts := []string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
		const tableCnt = 50_000.0
		// The base scan reads tenant_id and evaluates one predicate. The hidden
		// scan reads the 23-byte encoded key, emits tenant_id, and evaluates its
		// lookup predicate. Construct the exact equality boundary from those
		// independently known physical stages.
		const baseRowWork = 4.0 + regularIndexPredicateRowWork
		const candidateRowWork = 23.0 + 4.0 + regularIndexPredicateRowWork
		candidateRows := tableCnt * baseRowWork / candidateRowWork
		filter := makeRangeFilterExpr(0, 0, "=", 7)
		filter.Selectivity = candidateRows / tableCnt
		builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
			t, parts, []*planpb.Expr{filter},
			&planpb.Stats{TableCnt: tableCnt, Outcnt: candidateRows, Selectivity: filter.Selectivity, Cost: tableCnt},
			map[int32]int{0: 1}, false,
		)
		node := builder.qry.Nodes[scanID]
		node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		leadingPos := []int32{0}
		costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
		work, skip, valid := costCtx.score(idxDef, leadingPos, nil, encodedRegularIndexCostIndexOnly)
		require.True(t, valid)
		require.InDelta(t, costCtx.baseWork, work, 1e-9)
		require.True(t, skip)
	})

	t.Run("base scan work uses block-granular estimate", func(t *testing.T) {
		const tableCnt = 2_000_000.0
		const outputRows = 60.0
		filter := makeIntBetweenFilterExpr(0, 0, 100, 105)
		filter.Selectivity = outputRows / tableCnt
		builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
			t,
			[]string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
			[]*planpb.Expr{filter},
			&planpb.Stats{
				TableCnt: tableCnt, Outcnt: outputRows, Selectivity: filter.Selectivity,
				Cost: outputRows, BlockNum: 1,
			},
			map[int32]int{0: 1},
			false,
		)
		node := builder.qry.Nodes[scanID]
		node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
		work, skip, valid := costCtx.score(
			idxDef, []int32{0}, nil, encodedRegularIndexCostIndexOnly,
		)
		require.True(t, valid)
		require.Equal(t, float64(objectio.BlockMaxRows), costCtx.baseRows)
		require.Less(t, work, costCtx.baseWork)
		require.False(t, skip)
	})

	t.Run("prepared range fails open against base scan", func(t *testing.T) {
		const tableCnt = 2_000_000.0
		const outputRows = 200.0
		filter := makeParamBetweenFilterExpr(0, 0, 0, 1)
		filter.Selectivity = outputRows / tableCnt
		builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
			t,
			[]string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
			[]*planpb.Expr{filter},
			&planpb.Stats{
				TableCnt: tableCnt, Outcnt: outputRows, Selectivity: filter.Selectivity,
				Cost: 10_000, BlockNum: 2,
			},
			map[int32]int{0: 1},
			false,
		)
		node := builder.qry.Nodes[scanID]
		node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
		rankingWork, skip, valid := costCtx.score(
			idxDef, []int32{0}, nil, encodedRegularIndexCostIndexOnly,
		)
		require.True(t, valid)
		require.Greater(t, rankingWork, costCtx.baseWork,
			"neutral prepared-range work remains available for sibling ranking")
		require.False(t, skip,
			"unknown prepared bounds cannot prove the base scan is cheaper")
	})

	t.Run("prepared residual preserves interval-dominant rejection", func(t *testing.T) {
		equality := makeRangeFilterExpr(0, 0, "=", 7)
		equality.Selectivity = 0.5
		runtimeRange := makeParamBetweenFilterExpr(0, 3, 0, 1)
		runtimeRange.Selectivity = 0.13
		builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
			t,
			[]string{"tenant_id", "event_time", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
			[]*planpb.Expr{equality, runtimeRange},
			&planpb.Stats{
				TableCnt: 800_000, Outcnt: 52_000, Selectivity: 0.065,
				Cost: 10_000, BlockNum: 1,
			},
			map[int32]int{0: 1, 3: 1},
			false,
		)
		node := builder.qry.Nodes[scanID]
		for _, filter := range node.FilterList {
			filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		}
		costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
		work, skip, valid := costCtx.score(
			idxDef, []int32{0}, nil, encodedRegularIndexCostIndexOnly,
		)
		require.True(t, valid)
		require.Greater(t, work, costCtx.baseWork)
		require.True(t, skip,
			"stable index input work exceeds even a full base scan")
	})

	t.Run("index-only mixed OR residual preserves stable output work", func(t *testing.T) {
		newCase := func(t testing.TB, residual *planpb.Expr, outputRows float64) (*encodedRegularIndexCostContext, *planpb.IndexDef) {
			leading := makeStringEqFilterExpr(0, 2, "READY")
			leading.Selectivity = 0.3
			builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
				t,
				[]string{"category", "tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
				[]*planpb.Expr{leading, residual},
				&planpb.Stats{
					TableCnt: 800_000, Outcnt: outputRows, Selectivity: outputRows / 800_000,
					Cost: 10_000, BlockNum: 1,
				},
				map[int32]int{0: 1, 2: 1},
				false,
			)
			return builder.newEncodedRegularIndexCostContext(builder.qry.Nodes[scanID], colRefCnt), idxDef
		}

		known := makeIntBetweenFilterExpr(0, 0, 1, 20)
		known.Selectivity = 0.3
		mixed := makeOrFilterExpr(known, makeParamBetweenFilterExpr(0, 0, 0, 1))
		mixed.Selectivity = 0.37
		costCtx, idxDef := newCase(t, mixed, 88_800)
		_, skip, valid := costCtx.score(idxDef, []int32{0}, nil, encodedRegularIndexCostIndexOnly)
		require.True(t, valid)
		require.True(t, skip,
			"the stable residual output makes the index lower work exceed a full base scan")

		unknownOnly := makeOrFilterExpr(
			makeParamBetweenFilterExpr(0, 0, 0, 1),
			makeParamBetweenFilterExpr(0, 0, 2, 3),
		)
		unknownOnly.Selectivity = 0.19
		costCtx, idxDef = newCase(t, unknownOnly, 45_600)
		_, skip, valid = costCtx.score(idxDef, []int32{0}, nil, encodedRegularIndexCostIndexOnly)
		require.True(t, valid)
		require.False(t, skip, "an all-parameter residual may remove every hidden row")
	})

	t.Run("unpushable prepared residual preserves backfill lower bound", func(t *testing.T) {
		equality := makeRangeFilterExpr(0, 0, "=", 7)
		equality.Selectivity = 0.5
		runtimeRange := makeParamBetweenFilterExpr(0, 3, 0, 1)
		runtimeRange.Selectivity = 0.13
		builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
			t,
			[]string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
			[]*planpb.Expr{equality, runtimeRange},
			&planpb.Stats{
				TableCnt: 800_000, Outcnt: 52_000, Selectivity: 0.065,
				Cost: 10_000, BlockNum: 1,
			},
			map[int32]int{1: 1},
			false,
		)
		node := builder.qry.Nodes[scanID]
		for _, filter := range node.FilterList {
			filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		}
		costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
		work, skip, valid := costCtx.score(
			idxDef, []int32{0}, nil, encodedRegularIndexCostBackfill,
		)
		require.True(t, valid)
		require.Greater(t, work, costCtx.baseWork)
		require.True(t, skip,
			"a residual unavailable to the index cannot reduce index rows or base lookups")
	})

	t.Run("nullable PK leading residual charges physical column", func(t *testing.T) {
		filter := makeParamEqFilterExpr(0, 4, 0)
		filter.Selectivity = 0.3
		builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
			t,
			[]string{catalog.CreateAlias(catalog.CPrimaryKeyColName), "tenant_id"},
			[]*planpb.Expr{filter},
			&planpb.Stats{TableCnt: 800_000, Outcnt: 240_000, Selectivity: 0.3, Cost: 800_000},
			map[int32]int{4: 1},
			false,
		)
		node := builder.qry.Nodes[scanID]
		node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		leadingPos := []int32{0}
		require.False(t, builder.shouldSkipEncodedIndexOnlyScan(idxDef, node, colRefCnt, leadingPos, false))
		require.True(t, builder.shouldSkipEncodedIndexOnlyScan(idxDef, node, colRefCnt, leadingPos, true))
	})

	t.Run("scan hints", func(t *testing.T) {
		t.Run("USE remains costed", func(t *testing.T) {
			builder, scanID, idxDef, colRefCnt := newBroadCase(t, 800_000)
			builder.indexHintsByScan[scanID] = &indexHintSet{scan: indexHintScopeSet{
				useSpecified: true, use: map[string]struct{}{idxDef.IndexName: {}},
			}}
			require.Equal(t, int32(-1), builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{}))
		})
		t.Run("IGNORE removes candidate", func(t *testing.T) {
			builder, scanID, idxDef, _ := newBroadCase(t, 800_000)
			builder.indexHintsByScan[scanID] = &indexHintSet{scan: indexHintScopeSet{
				ignore: map[string]struct{}{idxDef.IndexName: {}},
			}}
			require.Empty(t, builder.filterRegularIndexesByScanHints(builder.qry.Nodes[scanID], []*planpb.IndexDef{idxDef}))
		})
		t.Run("FORCE bypasses cost", func(t *testing.T) {
			builder, scanID, idxDef, colRefCnt := newBroadCase(t, 800_000)
			builder.indexHintsByScan[scanID] = &indexHintSet{scan: indexHintScopeSet{
				forceSpecified: true, force: map[string]struct{}{idxDef.IndexName: {}},
			}}
			require.NotEqual(t, int32(-1), builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{}))
		})
	})

	t.Run("index shape controls", func(t *testing.T) {
		t.Run("non-covering index is rejected before costing", func(t *testing.T) {
			filter := makeRangeFilterExpr(0, 0, "=", 7)
			filter.Selectivity = 0.001
			builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
				t, []string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, []*planpb.Expr{filter},
				&planpb.Stats{TableCnt: 800_000, Outcnt: 800, Selectivity: 0.001, Cost: 800_000},
				map[int32]int{0: 1, 1: 1}, false,
			)
			node := builder.qry.Nodes[scanID]
			node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
			require.Equal(t, int32(-1), builder.tryIndexOnlyScan(idxDef, node, colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{}))
		})
		t.Run("single-column unique index remains direct", func(t *testing.T) {
			filter := makeRangeFilterExpr(0, 0, "=", 7)
			filter.Selectivity = 0.9
			builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
				t, []string{"tenant_id"}, []*planpb.Expr{filter},
				&planpb.Stats{TableCnt: 800_000, Outcnt: 720_000, Selectivity: 0.9, Cost: 800_000},
				map[int32]int{0: 1}, false,
			)
			idxDef.Unique = true
			node := builder.qry.Nodes[scanID]
			node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
			require.NotEqual(t, int32(-1), builder.tryIndexOnlyScan(idxDef, node, colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{}))
		})
		t.Run("single-user-column non-unique index keeps selective win", func(t *testing.T) {
			filter := makeRangeFilterExpr(0, 0, "=", 7)
			filter.Selectivity = 0.001
			builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
				t, []string{"tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, []*planpb.Expr{filter},
				&planpb.Stats{TableCnt: 800_000, Outcnt: 800, Selectivity: 0.001, Cost: 800_000},
				map[int32]int{0: 1}, false,
			)
			node := builder.qry.Nodes[scanID]
			node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
			require.NotEqual(t, int32(-1), builder.tryIndexOnlyScan(idxDef, node, colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{}))
		})
	})
}

func TestEncodedRegularIndexCostShapeAndPaginationControls(t *testing.T) {
	wideParts := []string{
		"category", "tenant_id", "event_time", "event_id",
		catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}
	newCase := func(t testing.TB, selectivity, outcnt, cost float64, force bool) (*QueryBuilder, *planpb.Node, *planpb.IndexDef, map[[2]int32]int) {
		filter := makeStringEqFilterExpr(0, 2, "READY")
		filter.Selectivity = selectivity
		builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
			t, wideParts, []*planpb.Expr{filter},
			&planpb.Stats{TableCnt: 1_000_000, Outcnt: outcnt, Selectivity: selectivity, Cost: cost},
			map[int32]int{1: 1, 2: 1}, force,
		)
		node := builder.qry.Nodes[scanID]
		node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		return builder, node, idxDef, colRefCnt
	}

	t.Run("backfill sibling uses common decision", func(t *testing.T) {
		for _, test := range []struct {
			name        string
			selectivity float64
			outcnt      float64
			force       bool
			wantSkip    bool
		}{
			{name: "broad rejects", selectivity: 0.20, outcnt: 120_000, wantSkip: true},
			{name: "selective wins", selectivity: 0.001, outcnt: 1_000},
			{name: "force preserves broad", selectivity: 0.20, outcnt: 120_000, force: true},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder, node, idxDef, colRefCnt := newCase(t, test.selectivity, test.outcnt, 1_000_000, test.force)
				require.Equal(t, test.wantSkip, builder.shouldSkipEncodedRegularIndex(
					idxDef, node, colRefCnt, []int32{0}, false, encodedRegularIndexCostBackfill,
				))
			})
		}
	})

	t.Run("exact leading pagination caps hidden candidates", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			limit  *planpb.Expr
			offset *planpb.Expr
			skip   bool
		}{
			{name: "without limit rejects", skip: true},
			{name: "limit", limit: makePlan2Uint64ConstExprWithType(1_000)},
			{name: "limit and offset", limit: makePlan2Uint64ConstExprWithType(1_000), offset: makePlan2Uint64ConstExprWithType(500)},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder, node, idxDef, colRefCnt := newCase(t, 0.05, 1_000, 24_576, false)
				node.Limit, node.Offset = test.limit, test.offset
				require.Equal(t, test.skip, builder.shouldSkipEncodedIndexOnlyScan(idxDef, node, colRefCnt, []int32{0}, false))
			})
		}
	})

	t.Run("unsafe pagination estimates use full candidate rows", func(t *testing.T) {
		builder, node, idxDef, colRefCnt := newCase(t, 0.05, 1_000, 24_576, false)
		node.Limit = &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
		require.True(t, builder.shouldSkipEncodedIndexOnlyScan(idxDef, node, colRefCnt, []int32{0}, false))

		residual := makeRangeFilterExpr(node.BindingTags[0], 0, ">=", 1)
		residual.Selectivity = 0.5
		node.FilterList = append(node.FilterList, residual)
		node.Limit = makePlan2Uint64ConstExprWithType(1_000)
		require.True(t, builder.shouldSkipEncodedIndexOnlyScan(idxDef, node, colRefCnt, []int32{0}, false))
	})
}

func BenchmarkEncodedIndexCostWideTableMultiIndex(b *testing.B) {
	filter := makeStringEqFilterExpr(0, 2, "HOT")
	filter.Selectivity = 0.2
	builder, scanID, _, colRefCnt := newEncodedIndexCostTestCase(
		b,
		[]string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{filter},
		&planpb.Stats{TableCnt: 1_000_000, Outcnt: 200_000, Selectivity: 0.2, Cost: 1_000_000},
		map[int32]int{1: 2, 2: 1},
		false,
	)
	node := builder.qry.Nodes[scanID]
	relPos := node.BindingTags[0]
	node.FilterList[0].GetF().Args[0].GetCol().RelPos = relPos

	ctx := builder.compCtx.(*encodedIndexCostTestContext)
	statsWrapper := ctx.statsCache.Get(encodedIndexCostUnitTableID)
	stats := statsWrapper.GetStats()
	for colPos := len(node.TableDef.Cols); colPos < 128; colPos++ {
		name := fmt.Sprintf("cost_col_%03d", colPos)
		node.TableDef.Name2ColIndex[name] = int32(colPos)
		node.TableDef.Cols = append(node.TableDef.Cols, &planpb.ColDef{
			Name: name,
			Typ:  planpb.Type{Id: int32(types.T_int64)},
		})
		stats.SizeMap[name] = 8_000_000
	}

	parts := []string{"category"}
	for colPos := int32(5); colPos < 13; colPos++ {
		name := node.TableDef.Cols[colPos].Name
		parts = append(parts, name)
		colRefCnt[[2]int32{relPos, colPos}] = 1
		residual := makeRangeFilterExpr(relPos, colPos, ">=", 1)
		residual.Selectivity = 0.5
		node.FilterList = append(node.FilterList, residual)
	}
	parts = append(parts, catalog.CreateAlias(catalog.CPrimaryKeyColName))
	indexes := make([]*planpb.IndexDef, 16)
	for i := range indexes {
		indexes[i] = &planpb.IndexDef{
			IndexName: fmt.Sprintf("idx_cost_%02d", i),
			Parts:     slices.Clone(parts),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	rejected := false
	for i := 0; i < b.N; i++ {
		costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
		for _, idxDef := range indexes {
			if _, skip, _ := costCtx.score(idxDef, []int32{0}, nil, encodedRegularIndexCostIndexOnly); skip {
				rejected = true
			}
			if _, skip, _ := costCtx.score(idxDef, []int32{0}, nil, encodedRegularIndexCostBackfill); skip {
				rejected = true
			}
		}
	}
	if !rejected {
		b.Fatal("benchmark did not exercise the rejection path")
	}
}

func newEncodedExistsPlanTestContext(stateNDV float64) *encodedIndexPlanTestContext {
	const (
		activityTableID uint64 = 20_001
		tagsTableID     uint64 = 20_002
		indexTableID    uint64 = 20_003
	)
	baseCtx := NewMockCompilerContext(true)
	intType := planpb.Type{Id: int32(types.T_int32), NotNullable: true}
	bigintType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	stateType := planpb.Type{Id: int32(types.T_varchar), Width: 12, NotNullable: true}
	datetimeType := planpb.Type{Id: int32(types.T_datetime), NotNullable: true}
	smallintType := planpb.Type{Id: int32(types.T_int16), NotNullable: true}
	decimalType := planpb.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 3, NotNullable: true}
	amountType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	compoundType := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, NotNullable: true}
	rowIDType := planpb.Type{Id: int32(types.T_Rowid), Width: 16, NotNullable: true}

	activity := &planpb.TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     activityTableID,
		Name:      "cost_activity",
		Cols: []*planpb.ColDef{
			{ColId: 0, Name: "tenant_id", Typ: intType, Primary: true, Pkidx: 1, Default: &planpb.Default{}},
			{ColId: 1, Name: "activity_id", Typ: bigintType, Primary: true, Pkidx: 2, Default: &planpb.Default{}},
			{ColId: 2, Name: "state", Typ: stateType, Default: &planpb.Default{}},
			{ColId: 3, Name: "created_at", Typ: datetimeType, Default: &planpb.Default{}},
			{ColId: 4, Name: "amount", Typ: amountType, Default: &planpb.Default{}},
			{ColId: 5, Name: catalog.CPrimaryKeyColName, Typ: compoundType, Hidden: true, Default: &planpb.Default{}},
			{ColId: 6, Name: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &planpb.Default{}},
		},
		Name2ColIndex: map[string]int32{
			"tenant_id": 0, "activity_id": 1, "state": 2, "created_at": 3, "amount": 4,
			catalog.CPrimaryKeyColName: 5, catalog.Row_ID: 6,
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Cols:        []uint64{0, 1},
			Names:       []string{"tenant_id", "activity_id"},
			CompPkeyCol: &planpb.ColDef{Name: catalog.CPrimaryKeyColName, Typ: compoundType, Hidden: true},
		},
		Indexes: []*planpb.IndexDef{{
			IndexName:      "idx_state_tenant_time_id",
			IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
			IndexTableName: "__mo_idx_state_tenant_time_id",
			Parts: []string{
				"state", "tenant_id", "created_at", "activity_id",
				catalog.CreateAlias(catalog.CPrimaryKeyColName),
			},
			TableExist: true,
		}},
	}
	tags := &planpb.TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     tagsTableID,
		Name:      "cost_tags",
		Cols: []*planpb.ColDef{
			{ColId: 0, Name: "tenant_id", Typ: intType, Primary: true, Pkidx: 1, Default: &planpb.Default{}},
			{ColId: 1, Name: "activity_id", Typ: bigintType, Primary: true, Pkidx: 2, Default: &planpb.Default{}},
			{ColId: 2, Name: "tag_id", Typ: smallintType, Primary: true, Pkidx: 3, Default: &planpb.Default{}},
			{ColId: 3, Name: "weight", Typ: decimalType, Default: &planpb.Default{}},
			{ColId: 4, Name: catalog.CPrimaryKeyColName, Typ: compoundType, Hidden: true, Default: &planpb.Default{}},
			{ColId: 5, Name: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &planpb.Default{}},
		},
		Name2ColIndex: map[string]int32{
			"tenant_id": 0, "activity_id": 1, "tag_id": 2, "weight": 3,
			catalog.CPrimaryKeyColName: 4, catalog.Row_ID: 5,
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Cols:        []uint64{0, 1, 2},
			Names:       []string{"tenant_id", "activity_id", "tag_id"},
			CompPkeyCol: &planpb.ColDef{Name: catalog.CPrimaryKeyColName, Typ: compoundType, Hidden: true},
		},
	}
	indexTable := makeTestIndexTableDef()
	indexTable.TableType = catalog.SystemIndexRel
	indexTable.TblId = indexTableID
	indexTable.Name = activity.Indexes[0].IndexTableName
	indexTable.Pkey = &planpb.PrimaryKeyDef{
		PkeyColName: catalog.IndexTableIndexColName,
		Cols:        []uint64{0},
		Names:       []string{catalog.IndexTableIndexColName},
	}

	for _, tableDef := range []*planpb.TableDef{activity, tags, indexTable} {
		baseCtx.objects[tableDef.Name] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: tableDef.Name, Obj: int64(tableDef.TblId)}
		baseCtx.tables[tableDef.Name] = tableDef
		baseCtx.id2name[tableDef.TblId] = tableDef.Name
	}
	baseCtx.pks[activity.Name] = []int{0, 1}
	baseCtx.pks[tags.Name] = []int{0, 1, 2}
	baseCtx.pks[indexTable.Name] = []int{0}

	activityStats := NewStatsInfo()
	activityStats.TableCnt = 600_000
	activityStats.BlockNumber = 120
	activityStats.AccurateObjectNumber = 1
	activityStats.NdvMap = map[string]float64{
		"tenant_id": 64, "activity_id": 600_000, "state": stateNDV,
		"created_at": 600_000, "amount": 100_000, catalog.CPrimaryKeyColName: 600_000,
	}
	activityStats.MinValMap = map[string]float64{"tenant_id": 1, "activity_id": 1}
	activityStats.MaxValMap = map[string]float64{"tenant_id": 64, "activity_id": 600_000}
	activityStats.SizeMap = map[string]uint64{
		"tenant_id": 600_000 * 4, "activity_id": 600_000 * 8,
		"state": 600_000 * 5, "created_at": 600_000 * 8, "amount": 600_000 * 8,
		catalog.CPrimaryKeyColName: 600_000 * 14,
	}
	tagsStats := NewStatsInfo()
	tagsStats.TableCnt = 1_200_000
	tagsStats.BlockNumber = 240
	tagsStats.AccurateObjectNumber = 1
	tagsStats.NdvMap = map[string]float64{
		"tenant_id": 64, "activity_id": 600_000, "tag_id": 2, "weight": 10_000,
		catalog.CPrimaryKeyColName: 1_200_000,
	}
	tagsStats.MinValMap = map[string]float64{"tenant_id": 1, "activity_id": 1, "tag_id": 1, "weight": 0}
	tagsStats.MaxValMap = map[string]float64{"tenant_id": 64, "activity_id": 600_000, "tag_id": 2, "weight": 100}
	tagsStats.SizeMap = map[string]uint64{
		"tenant_id": 1_200_000 * 4, "activity_id": 1_200_000 * 8,
		"tag_id": 1_200_000 * 2, "weight": 1_200_000 * 8,
		catalog.CPrimaryKeyColName: 1_200_000 * 17,
	}
	indexStats := NewStatsInfo()
	indexStats.TableCnt = 600_000
	indexStats.BlockNumber = 120
	indexStats.AccurateObjectNumber = 1
	indexStats.NdvMap = map[string]float64{catalog.IndexTableIndexColName: 600_000, catalog.IndexTablePrimaryColName: 600_000}
	indexStats.SizeMap = map[string]uint64{catalog.IndexTableIndexColName: 600_000 * 52, catalog.IndexTablePrimaryColName: 600_000 * 14}

	statsCache := NewStatsCache()
	statsByID := map[uint64]*statspb.StatsInfo{
		activityTableID: activityStats,
		tagsTableID:     tagsStats,
		indexTableID:    indexStats,
	}
	for tableID, stats := range statsByID {
		statsCache.Set(tableID, stats)
	}
	return &encodedIndexPlanTestContext{MockCompilerContext: baseCtx, statsCache: statsCache, statsByID: statsByID}
}

func addCostActivityRegularIndex(t testing.TB, ctx *encodedIndexPlanTestContext, name string, parts []string, first bool) {
	t.Helper()
	activity := ctx.tables["cost_activity"]
	require.NotNil(t, activity)
	idxDef := &planpb.IndexDef{
		IndexName: name, IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
		IndexTableName: "__mo_" + name, Parts: slices.Clone(parts), TableExist: true,
	}
	if first {
		activity.Indexes = append([]*planpb.IndexDef{idxDef}, activity.Indexes...)
	} else {
		activity.Indexes = append(activity.Indexes, idxDef)
	}

	indexTable := makeTestIndexTableDef()
	indexTable.TableType = catalog.SystemIndexRel
	indexTable.TblId = uint64(21_000 + len(ctx.statsByID))
	indexTable.Name = idxDef.IndexTableName
	indexTable.Pkey = &planpb.PrimaryKeyDef{
		PkeyColName: catalog.IndexTableIndexColName,
		Cols:        []uint64{0},
		Names:       []string{catalog.IndexTableIndexColName},
	}
	ctx.objects[indexTable.Name] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: indexTable.Name, Obj: int64(indexTable.TblId)}
	ctx.tables[indexTable.Name] = indexTable
	ctx.id2name[indexTable.TblId] = indexTable.Name
	ctx.pks[indexTable.Name] = []int{0}
	stats := NewStatsInfo()
	stats.TableCnt = 600_000
	stats.BlockNumber = 120
	stats.AccurateObjectNumber = 1
	stats.NdvMap = map[string]float64{catalog.IndexTableIndexColName: 600_000, catalog.IndexTablePrimaryColName: 600_000}
	stats.SizeMap = map[string]uint64{catalog.IndexTableIndexColName: 600_000 * 24, catalog.IndexTablePrimaryColName: 600_000 * 14}
	ctx.statsByID[indexTable.TblId] = stats
	ctx.statsCache.Set(indexTable.TblId, stats)
}

func TestEncodedIndexCostChoosesProfitableSiblingIndependentOfDDLOrder(t *testing.T) {
	narrowParts := []string{"state", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
	for _, query := range []struct {
		name string
		sql  string
	}{
		{name: "point backfill", sql: "select amount from cost_activity where state = 'READY'"},
		{name: "range backfill", sql: "select amount from cost_activity where state between 'HOLD' and 'READY'"},
	} {
		for _, narrowFirst := range []bool{true, false} {
			order := "wide first"
			if narrowFirst {
				order = "narrow first"
			}
			t.Run(query.name+"/"+order, func(t *testing.T) {
				ctx := newEncodedExistsPlanTestContext(6)
				addCostActivityRegularIndex(t, ctx, "idx_state_narrow", narrowParts, narrowFirst)
				optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
				queryPlan, err := runOneStmt(optimizer, t, query.sql)
				require.NoError(t, err)
				require.Equal(t, "idx_state_narrow", findFirstIndexScanName(queryPlan))
				require.True(t, planHasIndexJoin(queryPlan))
			})
		}
	}
}

func TestEncodedIndexCostChoosesNarrowCoveringSiblingIndependentOfDDLOrder(t *testing.T) {
	wideParts := []string{
		"state", "tenant_id", "created_at", "activity_id",
		catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}
	narrowParts := []string{"state", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
	for _, narrowFirst := range []bool{true, false} {
		order := "wide first"
		if narrowFirst {
			order = "narrow first"
		}
		t.Run(order, func(t *testing.T) {
			ctx := newEncodedExistsPlanTestContext(100)
			ctx.tables["cost_activity"].Indexes = nil
			addCostActivityRegularIndex(t, ctx, "idx_state_narrow_covering", narrowParts, narrowFirst)
			addCostActivityRegularIndex(t, ctx, "idx_state_wide_covering", wideParts, !narrowFirst)

			queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t,
				"select state from cost_activity where state = 'READY'")
			require.NoError(t, err)
			require.Equal(t, "idx_state_narrow_covering", findFirstIndexScanName(queryPlan))
			require.False(t, planHasIndexJoin(queryPlan))
		})
	}
}

func TestEncodedIndexCostChoosesAcrossCoveringAndBackfill(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(50)
	ctx.tables["cost_activity"].Indexes = nil
	addCostActivityRegularIndex(t, ctx, "idx_state_wide_covering", []string{
		"state", "tenant_id", "created_at", "activity_id", "amount",
		catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)
	addCostActivityRegularIndex(t, ctx, "idx_state_narrow_backfill", []string{
		"state", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)

	queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t,
		"select amount from cost_activity where state = 'READY'")
	require.NoError(t, err)
	require.Equal(t, "idx_state_narrow_backfill", findFirstIndexScanName(queryPlan))
	require.True(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexCostChoosesAcrossPointAndRange(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(6)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	stats := ctx.statsByID[activity.TblId]
	stats.NdvMap["amount"] = stats.TableCnt
	stats.MinValMap["amount"] = 0
	stats.MaxValMap["amount"] = stats.TableCnt
	addCostActivityRegularIndex(t, ctx, "idx_state_point", []string{
		"state", "tenant_id", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)
	addCostActivityRegularIndex(t, ctx, "idx_amount_range", []string{
		"amount", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)

	queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
		select state, amount
		from cost_activity
		where state = 'READY' and amount between 1 and 1000`)
	require.NoError(t, err)
	require.Equal(t, "idx_amount_range", findFirstIndexScanName(queryPlan))
	require.True(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexCostPreparedRangeDoesNotOutrankEqualityByHeuristic(t *testing.T) {
	const tableCnt = 300_000
	for _, equalityFirst := range []bool{true, false} {
		order := "range index first"
		if equalityFirst {
			order = "equality index first"
		}
		t.Run(order, func(t *testing.T) {
			ctx := newEncodedExistsPlanTestContext(10)
			activity := ctx.tables["cost_activity"]
			activity.Indexes = nil
			stats := ctx.statsByID[activity.TblId]
			stats.TableCnt = tableCnt
			stats.BlockNumber = 60
			stats.NdvMap["state"] = 5_000
			stats.NdvMap["amount"] = tableCnt
			stats.MinValMap["amount"] = 0
			stats.MaxValMap["amount"] = tableCnt
			stats.SizeMap = map[string]uint64{
				"tenant_id": tableCnt * 4, "activity_id": tableCnt * 8,
				"state": tableCnt * 5, "created_at": tableCnt * 8, "amount": tableCnt * 8,
				catalog.CPrimaryKeyColName: tableCnt * 14,
			}

			equalityParts := []string{
				"state", "amount", catalog.CreateAlias(catalog.CPrimaryKeyColName),
			}
			rangeParts := []string{
				"amount", "state", catalog.CreateAlias(catalog.CPrimaryKeyColName),
			}
			if equalityFirst {
				addCostActivityRegularIndex(t, ctx, "idx_state_amount_pk", equalityParts, false)
				addCostActivityRegularIndex(t, ctx, "idx_amount_state_pk", rangeParts, false)
			} else {
				addCostActivityRegularIndex(t, ctx, "idx_amount_state_pk", rangeParts, false)
				addCostActivityRegularIndex(t, ctx, "idx_state_amount_pk", equalityParts, false)
			}

			optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
			literalPlan, err := runOneStmt(optimizer, t, `
				select state from cost_activity
				where state = 'READY' and amount between 1000 and 5000`)
			require.NoError(t, err)
			require.Equal(t, "idx_state_amount_pk", findFirstIndexScanName(literalPlan))

			preparePlan, err := runOneStmt(optimizer, t,
				"prepare cost_parameter_range from 'select state from cost_activity where state = ? and amount between ? and ?'")
			require.NoError(t, err)
			preparedPlan := resolveQueryPlan(preparePlan)
			preparedIndex := findFirstIndexScanNode(preparedPlan)
			require.NotNil(t, preparedIndex)
			require.Equal(t, "idx_state_amount_pk", preparedIndex.IndexScanInfo.IndexName)
			hasParamRef := false
			for _, filter := range preparedIndex.FilterList {
				hasParamRef = hasParamRef || containsDynamicParam(filter)
			}
			require.True(t, hasParamRef, "the prepared public plan must retain ParamRefs")
		})
	}
}

func TestEncodedIndexCostPreparedRangesUseUncertaintyFromPublicPlan(t *testing.T) {
	const tableCnt = 2_000_000
	ctx := newEncodedExistsPlanTestContext(10)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	stats := ctx.statsByID[activity.TblId]
	stats.TableCnt = tableCnt
	stats.BlockNumber = 240
	stats.NdvMap["amount"] = tableCnt
	stats.MinValMap["amount"] = 0
	stats.MaxValMap["amount"] = tableCnt
	stats.SizeMap = map[string]uint64{
		"tenant_id": tableCnt * 4, "activity_id": tableCnt * 8,
		"state": tableCnt * 5, "created_at": tableCnt * 8, "amount": tableCnt * 8,
		catalog.CPrimaryKeyColName: tableCnt * 14,
	}
	addCostActivityRegularIndex(t, ctx, "idx_amount_pk", []string{
		"amount", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)
	optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}

	preparePlan, err := runOneStmt(optimizer, t, `
		prepare cost_random_ranges from '
			select count(amount) from cost_activity
			where amount between ? and ?
			   or amount between ? and ?
			   or amount between ? and ?'`)
	require.NoError(t, err)
	preparedPlan := resolveQueryPlan(preparePlan)
	preparedIndex := findFirstIndexScanNode(preparedPlan)
	require.NotNil(t, preparedIndex,
		"overlapping cost intervals retain the ranked index candidate")
	require.Equal(t, "idx_amount_pk", preparedIndex.IndexScanInfo.IndexName)
	require.False(t, planHasIndexJoin(preparedPlan),
		"a covering candidate must not backfill through the same index")
	require.Len(t, preparedIndex.FilterList, 1,
		"runtime NULL bounds must not require decoded row residuals")
	require.Equal(t, 6, countExprFunctionCalls(preparedIndex.FilterList, "serial"))
	require.Zero(t, countExprFunctionCalls(preparedIndex.FilterList, "serial_full"))
	require.Zero(t, countExprFunctionCalls(preparedIndex.FilterList, "isnotnull"))
	require.Zero(t, countExprFunctionCalls(preparedIndex.FilterList, "serial_extract"))
	hasParamRef := false
	for _, filter := range preparedIndex.FilterList {
		hasParamRef = hasParamRef || containsDynamicParam(filter)
	}
	require.True(t, hasParamRef, "the prepared public plan must retain ParamRefs")
}

func TestEncodedIndexCostPreparedRangeStillUsesNonCoveringIndex(t *testing.T) {
	const tableCnt = 2_000_000
	ctx := newEncodedExistsPlanTestContext(10)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	stats := ctx.statsByID[activity.TblId]
	stats.TableCnt = tableCnt
	stats.BlockNumber = 240
	stats.NdvMap["amount"] = tableCnt
	stats.MinValMap["amount"] = 0
	stats.MaxValMap["amount"] = tableCnt
	addCostActivityRegularIndex(t, ctx, "idx_amount_pk", []string{
		"amount", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)

	preparePlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
		prepare cost_noncovering_range from '
			select state from cost_activity
			where amount between ? and ?'`)
	require.NoError(t, err)
	preparedPlan := resolveQueryPlan(preparePlan)
	require.Equal(t, "idx_amount_pk", findFirstIndexScanName(preparedPlan))
	require.True(t, planHasIndexJoin(preparedPlan),
		"uncertainty must not disable a required non-covering range access")
}

func TestEncodedIndexCostPreparedMixedOrPreservesKnownWorkFromPublicPlan(t *testing.T) {
	const tableCnt = 600_000
	ctx := newEncodedExistsPlanTestContext(10)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	stats := ctx.statsByID[activity.TblId]
	stats.TableCnt = tableCnt
	stats.BlockNumber = 120
	stats.NdvMap["amount"] = tableCnt
	stats.MinValMap["amount"] = 0
	stats.MaxValMap["amount"] = tableCnt
	// Comparison-bound serialization removes the old NULL residual charge.
	// Keep this control decisively above the rejection crossover so it still
	// proves that stable OR-branch output work is present in the lower bound.
	stats.SizeMap["state"] = tableCnt * 8
	addCostActivityRegularIndex(t, ctx, "idx_amount_wide", []string{
		"amount", "state", "created_at", "tenant_id", "activity_id",
		catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)

	preparePlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
		prepare cost_mixed_ranges from '
			select count(amount) from cost_activity
			where amount between 0 and 60000
			   or amount between ? and ?'`)
	require.NoError(t, err)
	preparedPlan := resolveQueryPlan(preparePlan)
	require.Empty(t, findFirstIndexScanName(preparedPlan),
		"the known OR branch alone makes the wide encoded scan more expensive than a base scan")
}

func TestEncodedIndexRangeLowerSelectivityComposesStableOrBranches(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	knownNarrow := makeIntBetweenFilterExpr(0, 0, 1, 10)
	knownNarrow.Selectivity = 0.2
	knownWide := makeIntBetweenFilterExpr(0, 0, 1, 20)
	knownWide.Selectivity = 0.3
	unknown := makeParamBetweenFilterExpr(0, 0, 0, 1)

	lower, hasUnknown := encodedRegularIndexRangeLowerSelectivity(
		makeOrFilterExpr(makeOrFilterExpr(knownNarrow, unknown), knownWide), builder, nil,
	)
	require.True(t, hasUnknown)
	require.Equal(t, 0.3, lower,
		"OR uses its largest stable child without assuming that stable branches are disjoint")

	lower, hasUnknown = encodedRegularIndexRangeLowerSelectivity(
		makeOrFilterExpr(
			makeParamBetweenFilterExpr(0, 0, 0, 1),
			makeParamBetweenFilterExpr(0, 0, 2, 3),
		), builder, nil,
	)
	require.True(t, hasUnknown)
	require.Zero(t, lower, "an all-parameter OR may be empty")
}

func TestEncodedIndexCostKeepsCatalogOrderOnEqualScore(t *testing.T) {
	for _, first := range []string{"idx_state_a", "idx_state_b"} {
		t.Run(first, func(t *testing.T) {
			ctx := newEncodedExistsPlanTestContext(100)
			ctx.tables["cost_activity"].Indexes = nil
			second := "idx_state_b"
			if first == second {
				second = "idx_state_a"
			}
			parts := []string{"state", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
			addCostActivityRegularIndex(t, ctx, first, parts, false)
			addCostActivityRegularIndex(t, ctx, second, parts, false)

			queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t,
				"select state from cost_activity where state = 'READY'")
			require.NoError(t, err)
			require.Equal(t, first, findFirstIndexScanName(queryPlan))
		})
	}
}

func TestEncodedIndexCostPrefersValidCandidateOverUnscorable(t *testing.T) {
	invalidParts := []string{
		"state", "missing_catalog_column", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}
	validParts := []string{"amount", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
	for _, invalidFirst := range []bool{true, false} {
		order := "valid first"
		if invalidFirst {
			order = "invalid first"
		}
		t.Run(order, func(t *testing.T) {
			ctx := newEncodedExistsPlanTestContext(600_000)
			ctx.tables["cost_activity"].Indexes = nil
			stats := ctx.statsByID[ctx.tables["cost_activity"].TblId]
			stats.NdvMap["amount"] = 600_000
			if invalidFirst {
				addCostActivityRegularIndex(t, ctx, "idx_state_unscorable", invalidParts, false)
				addCostActivityRegularIndex(t, ctx, "idx_amount_valid", validParts, false)
			} else {
				addCostActivityRegularIndex(t, ctx, "idx_amount_valid", validParts, false)
				addCostActivityRegularIndex(t, ctx, "idx_state_unscorable", invalidParts, false)
			}

			queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
				select created_at
				from cost_activity
				where state = 'READY' and amount = 1`)
			require.NoError(t, err)
			require.Equal(t, "idx_amount_valid", findFirstIndexScanName(queryPlan))
			require.True(t, planHasIndexJoin(queryPlan))
		})
	}
}

func TestEncodedIndexCostDoesNotFallbackToUnscorableWhenValidCandidateRejects(t *testing.T) {
	invalidParts := []string{
		"state", "missing_catalog_column", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}
	validParts := []string{"amount", catalog.CreateAlias(catalog.CPrimaryKeyColName)}
	for _, invalidFirst := range []bool{true, false} {
		order := "valid first"
		if invalidFirst {
			order = "invalid first"
		}
		t.Run(order, func(t *testing.T) {
			ctx := newEncodedExistsPlanTestContext(1_000)
			ctx.tables["cost_activity"].Indexes = nil
			stats := ctx.statsByID[ctx.tables["cost_activity"].TblId]
			stats.NdvMap["amount"] = 10
			if invalidFirst {
				addCostActivityRegularIndex(t, ctx, "idx_state_unscorable", invalidParts, false)
				addCostActivityRegularIndex(t, ctx, "idx_amount_valid_broad", validParts, false)
			} else {
				addCostActivityRegularIndex(t, ctx, "idx_amount_valid_broad", validParts, false)
				addCostActivityRegularIndex(t, ctx, "idx_state_unscorable", invalidParts, false)
			}

			queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
				select activity_id
				from cost_activity
				where state = 'READY'
				  and amount = 1
				  and created_at = '2026-08-09 00:00:00'`)
			require.NoError(t, err)
			require.Empty(t, findFirstIndexScanName(queryPlan),
				"an unscorable fallback is allowed only when every matching candidate is unscorable")
			require.False(t, planHasIndexJoin(queryPlan))
		})
	}
}

func TestEncodedIndexCostKeepsCatalogFallbackWhenAllCandidatesUnscorable(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(600_000)
	ctx.tables["cost_activity"].Indexes = nil
	stats := ctx.statsByID[ctx.tables["cost_activity"].TblId]
	stats.NdvMap["amount"] = 600_000
	addCostActivityRegularIndex(t, ctx, "idx_state_unscorable", []string{
		"state", "missing_state_suffix", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)
	addCostActivityRegularIndex(t, ctx, "idx_amount_unscorable", []string{
		"amount", "missing_amount_suffix", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)

	queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
		select created_at
		from cost_activity
		where state = 'READY' and amount = 1`)
	require.NoError(t, err)
	require.Equal(t, "idx_state_unscorable", findFirstIndexScanName(queryPlan))
	require.True(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexCostChargesBaseRecheckedPushablePredicates(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(3.5)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	stats := ctx.statsByID[activity.TblId]
	stats.NdvMap["amount"] = 10
	stats.MinValMap["amount"] = 0
	stats.MaxValMap["amount"] = 10
	addCostActivityRegularIndex(t, ctx, "idx_state_amount", []string{
		"state", "amount", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)

	queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
		select activity_id
		from cost_activity
		where state = 'READY' and amount >= 1`)
	require.NoError(t, err)
	require.Empty(t, findFirstIndexScanName(queryPlan),
		"a hidden residual copy does not remove the predicate from the base child")
	require.False(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexDuplicatePartsUseCanonicalFilterMapping(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(1_000)
	ctx.tables["cost_activity"].Indexes = nil
	addCostActivityRegularIndex(t, ctx, "idx_state_repeated", []string{
		"state", "state", "activity_id", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)

	preparePlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t,
		"prepare cost_repeat from 'select 1 from cost_activity where state = ?'")
	require.NoError(t, err)
	queryPlan := resolveQueryPlan(preparePlan)
	idxNode := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, idxNode)
	require.Equal(t, "idx_state_repeated", idxNode.IndexScanInfo.IndexName)
	require.Len(t, idxNode.FilterList, 2,
		"the string prefix lookup still requires an exact decoded residual")
	assert.InDelta(t, 0.001, idxNode.FilterList[0].Selectivity, 1e-12)
	assert.InDelta(t, 1.0, idxNode.FilterList[1].Selectivity, 1e-12,
		"a semantic recheck must not reduce candidate cardinality a second time")
	assert.InDelta(t, 600.0, idxNode.Stats.Outcnt, 1e-9)
	assert.Zero(t, countExprFunctionCalls(idxNode.FilterList, "isnotnull"))
	assert.Equal(t, 1, countExprFunctionCalls(idxNode.FilterList, "serial_extract"))
	assert.Equal(t, 2, firstIndexLookupSerialArgCount(queryPlan),
		"duplicate physical parts still require duplicate encoded lookup arguments")
}

func countExprFunctionCalls(exprs []*planpb.Expr, name string) int {
	count := 0
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		switch impl := expr.Expr.(type) {
		case *planpb.Expr_F:
			if impl.F.Func != nil && impl.F.Func.ObjName == name {
				count++
			}
			for _, arg := range impl.F.Args {
				visit(arg)
			}
		case *planpb.Expr_List:
			for _, arg := range impl.List.List {
				visit(arg)
			}
		case *planpb.Expr_W:
			visit(impl.W.WindowFunc)
			for _, arg := range impl.W.PartitionBy {
				visit(arg)
			}
			for _, orderBy := range impl.W.OrderBy {
				visit(orderBy.Expr)
			}
		}
	}
	for _, expr := range exprs {
		visit(expr)
	}
	return count
}

func newEncodedPaginationPlanTestContext(t testing.TB, stateNDV float64) *encodedIndexPlanTestContext {
	t.Helper()
	ctx := newEncodedExistsPlanTestContext(stateNDV)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	stats := ctx.statsByID[activity.TblId]
	stats.NdvMap["amount"] = 100
	stats.MinValMap["amount"] = 0
	stats.MaxValMap["amount"] = 100
	addCostActivityRegularIndex(t, ctx, "idx_state_amount_activity", []string{
		"state", "amount", "activity_id", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)
	return ctx
}

func TestEncodedIndexCostDoesNotCapResidualLimit(t *testing.T) {
	ctx := newEncodedPaginationPlanTestContext(t, 3)
	queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
		select amount, activity_id
		from cost_activity
		where state = 'READY' and amount between 1 and 90
		order by amount, activity_id
		limit 1000`)
	require.NoError(t, err)
	assert.Empty(t, findFirstIndexScanName(queryPlan))
	assert.False(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexCostDoesNotCapPreparedLimit(t *testing.T) {
	ctx := newEncodedPaginationPlanTestContext(t, 2)
	preparePlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t,
		"prepare cost_limit from 'select state from cost_activity where state = ''READY'' limit ?'")
	require.NoError(t, err)
	queryPlan := resolveQueryPlan(preparePlan)
	require.Empty(t, findFirstIndexScanName(queryPlan))
	assert.False(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexCostDoesNotCapOverflowingOffset(t *testing.T) {
	ctx := newEncodedPaginationPlanTestContext(t, 2)
	queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
		select state
		from cost_activity
		where state = 'READY'
		limit 18446744073709551615 offset 1`)
	require.NoError(t, err)
	require.Empty(t, findFirstIndexScanName(queryPlan))
	assert.False(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexCostForceBypassesUnsafePaginationGuard(t *testing.T) {
	ctx := newEncodedPaginationPlanTestContext(t, 2)
	queryPlan, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, `
		select amount, activity_id
		from cost_activity force index (idx_state_amount_activity)
		where state = 'READY' and amount between 1 and 90
		limit 1000`)
	require.NoError(t, err)
	require.Equal(t, "idx_state_amount_activity", findFirstIndexScanName(queryPlan))
}

func TestForceIndexPreservesCoveringShapeOverCheaperBackfill(t *testing.T) {
	filter := makeParamEqFilterExpr(0, 2, 0)
	filter.Selectivity = 0.1
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		[]string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{filter},
		&planpb.Stats{TableCnt: 800_000, Outcnt: 80_000, Selectivity: 0.1, Cost: 800_000},
		map[int32]int{2: 10}, true,
	)
	node := builder.qry.Nodes[scanID]
	node.TableDef.Indexes = []*planpb.IndexDef{idxDef}
	node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]

	costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
	covering, ok := builder.matchRegularIndexOnlyScan(idxDef, node, costCtx)
	require.True(t, ok)
	backfillFilters := costCtx.matchPointBackfill(idxDef, true)
	require.NotEmpty(t, backfillFilters)
	coveringWork, _, coveringValid := costCtx.score(
		idxDef, covering.filterIdx, covering.residualLeadingPos, encodedRegularIndexCostIndexOnly,
	)
	backfillWork, _, backfillValid := costCtx.score(
		idxDef, backfillFilters, nil, encodedRegularIndexCostBackfill,
	)
	require.True(t, coveringValid)
	require.True(t, backfillValid)
	require.Less(t, backfillWork, coveringWork)

	resultID := builder.applyIndicesForFiltersRegularIndex(
		scanID, node, colRefCnt, map[[2]int32]*planpb.Expr{},
	)
	result := builder.qry.Nodes[resultID]
	require.Equal(t, planpb.Node_TABLE_SCAN, result.NodeType)
	require.True(t, result.IndexScanInfo.IsIndexScan)
	require.Equal(t, idxDef.IndexName, result.IndexScanInfo.IndexName)
}

func TestSmallTablePreservesCoveringShapeOverCheaperBackfill(t *testing.T) {
	filter := makeStringEqFilterExpr(0, 2, "joce")
	filter.Selectivity = 0.1
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		[]string{"category", "event_id"},
		[]*planpb.Expr{filter},
		&planpb.Stats{TableCnt: 1_000, Outcnt: 100, Selectivity: 0.1, Cost: 1_000},
		map[int32]int{1: 1}, false,
	)
	node := builder.qry.Nodes[scanID]
	node.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "event_id", Names: []string{"event_id"}}
	node.TableDef.Cols[2].Typ = planpb.Type{Id: int32(types.T_char), Width: 35}
	statsWrapper := builder.compCtx.(*encodedIndexCostTestContext).
		statsCache.Get(encodedIndexCostUnitTableID)
	stats := statsWrapper.GetStats()
	stats.SizeMap["category"] = uint64(stats.TableCnt) * 35
	node.TableDef.Indexes = []*planpb.IndexDef{idxDef}
	node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]

	costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
	covering, ok := builder.matchRegularIndexOnlyScan(idxDef, node, costCtx)
	require.True(t, ok)
	require.Equal(t, []int32{0}, covering.residualLeadingPos)
	backfillFilters := costCtx.matchPointBackfill(idxDef, true)
	require.NotEmpty(t, backfillFilters)
	coveringWork, _, coveringValid := costCtx.score(
		idxDef, covering.filterIdx, covering.residualLeadingPos, encodedRegularIndexCostIndexOnly,
	)
	backfillWork, _, backfillValid := costCtx.score(
		idxDef, backfillFilters, nil, encodedRegularIndexCostBackfill,
	)
	require.True(t, coveringValid)
	require.True(t, backfillValid)
	require.Less(t, backfillWork, coveringWork,
		"the regression requires cross-shape scoring to prefer backfill")

	resultID := builder.applyIndicesForFiltersRegularIndex(
		scanID, node, colRefCnt, map[[2]int32]*planpb.Expr{},
	)
	result := builder.qry.Nodes[resultID]
	require.Equal(t, planpb.Node_TABLE_SCAN, result.NodeType)
	require.True(t, result.IndexScanInfo.IsIndexScan)
	require.Equal(t, idxDef.IndexName, result.IndexScanInfo.IndexName)
}

func TestEncodedIndexCostChargesUnpushableResidualOnBackfillCandidates(t *testing.T) {
	leading := makeStringEqFilterExpr(0, 2, "READY")
	leading.Selectivity = 0.25
	residual := makeRangeFilterExpr(0, 3, "=", 7)
	residual.Selectivity = 0.0005
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		[]string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{leading, residual},
		&planpb.Stats{TableCnt: 800_000, Outcnt: 100, Selectivity: 0.000125, Cost: 400_000},
		map[int32]int{1: 1}, false,
	)
	node := builder.qry.Nodes[scanID]
	for _, filter := range node.FilterList {
		filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	}
	require.True(t, builder.shouldSkipEncodedRegularIndex(
		idxDef, node, colRefCnt, []int32{0}, false, encodedRegularIndexCostBackfill,
	))
}

func TestEncodedIndexCostDoesNotCreditLossyPrefixResidualPushdown(t *testing.T) {
	leading := makeStringEqFilterExpr(0, 2, "READY")
	leading.Selectivity = 0.25
	residual := makeStringEqFilterExpr(0, 3, "2026-08-10T12:00:00")
	residual.Selectivity = 0.0001
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		[]string{"category", "event_time", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{leading, residual},
		&planpb.Stats{TableCnt: 800_000, Outcnt: 20, Selectivity: 0.000025, Cost: 760_000},
		map[int32]int{1: 1}, false,
	)
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "event_time:4",
	})
	require.NoError(t, err)
	idxDef.IndexAlgoParams = prefixParams
	node := builder.qry.Nodes[scanID]
	node.TableDef.Cols[3].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 64}
	for _, filter := range node.FilterList {
		filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	}

	costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
	work, reject, valid := costCtx.score(
		idxDef, []int32{0}, nil, encodedRegularIndexCostBackfill,
	)
	require.True(t, valid)
	require.True(t, reject,
		"a lossy prefix residual must not make an otherwise expensive backfill look selective: work=%v base=%v", work, costCtx.baseWork)

	residual.Selectivity = 1
	controlWork, controlReject, controlValid := costCtx.score(
		idxDef, []int32{0}, nil, encodedRegularIndexCostBackfill,
	)
	require.True(t, controlValid)
	require.True(t, controlReject)
	require.Equal(t, controlWork, work,
		"cost must be invariant to the selectivity of a residual the index table cannot evaluate")

	idxTableNode := &planpb.Node{
		TableDef:    makeTestIndexTableDef(),
		BindingTags: []int32{builder.genNewBindTag()},
	}
	builder.applyExtraFiltersOnIndex(idxDef, node, idxTableNode, []int32{0})
	require.Empty(t, idxTableNode.FilterList,
		"the physical plan must agree with the cost model and leave the lossy predicate on the base scan")
}

func TestEncodedIndexCostChargesSerializedSimplePKResidual(t *testing.T) {
	leading := makeStringEqFilterExpr(0, 2, "READY")
	leading.Selectivity = 0.5
	residual := makeRangeFilterExpr(0, 1, "=", 7)
	residual.Selectivity = 0.0001
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		[]string{"category", catalog.CreateAlias("event_id")},
		[]*planpb.Expr{leading, residual},
		&planpb.Stats{TableCnt: 800_000, Outcnt: 40, Selectivity: 0.00005, Cost: 800_000},
		map[int32]int{0: 1}, false,
	)
	node := builder.qry.Nodes[scanID]
	node.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "event_id", Names: []string{"event_id"}}
	for _, filter := range node.FilterList {
		filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	}

	idxTableNode := &planpb.Node{
		TableDef:    makeTestIndexTableDef(),
		BindingTags: []int32{builder.genNewBindTag()},
	}
	builder.applyExtraFiltersOnIndex(idxDef, node, idxTableNode, []int32{0})
	require.Len(t, idxTableNode.FilterList, 1)
	mapped := idxTableNode.FilterList[0].GetF().Args[0]
	require.Equal(t, "serial_extract", wrappedSerialFuncName(t, mapped))
	require.Equal(t, int32(0), mapped.GetF().Args[0].GetCol().ColPos,
		"the appended simple PK is encoded in index-table column 0")

	_, reject, valid := builder.newEncodedRegularIndexCostContext(node, colRefCnt).score(
		idxDef, []int32{0}, nil, encodedRegularIndexCostBackfill,
	)
	require.True(t, valid)
	require.True(t, reject,
		"cost must include the serial_extract that the physical plan materializes")
}

func TestEncodedIndexCostCountsConsumersAfterProjectionElimination(t *testing.T) {
	optimizer := &encodedIndexPlanTestOptimizer{ctx: newEncodedExistsPlanTestContext(20)}
	queryPlan, err := runOneStmt(optimizer, t, `
		select activity_alias, activity_alias, activity_alias, activity_alias, activity_alias
		from (
			select activity_id as activity_alias, state
			from cost_activity
		) derived
		where state = 'READY'`)
	require.NoError(t, err)
	require.Equal(t, "idx_state_tenant_time_id", findFirstIndexScanName(queryPlan))
	require.True(t, planHasIndexJoin(queryPlan))
	require.Zero(t, countPlanFunctionCalls(queryPlan, "serial_extract"))
}

func TestEncodedIndexCostChargesCompoundPKResidualExtraction(t *testing.T) {
	leading := makeStringEqFilterExpr(0, 2, "READY")
	leading.Selectivity = 0.25
	residual := makeIntInFilterExpr(0, 0, 7, 8)
	residual.Selectivity = 0.0005
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		[]string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{leading, residual},
		&planpb.Stats{TableCnt: 800_000, Outcnt: 100, Selectivity: 0.000125, Cost: 500_000},
		map[int32]int{1: 1}, false,
	)
	node := builder.qry.Nodes[scanID]
	for _, filter := range node.FilterList {
		filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	}
	require.True(t, builder.shouldSkipEncodedRegularIndex(
		idxDef, node, colRefCnt, []int32{0}, false, encodedRegularIndexCostBackfill,
	))
}

func TestEncodedIndexCostDeduplicatesRepeatedLeadingFilterSelectivity(t *testing.T) {
	filter := makeStringEqFilterExpr(0, 2, "READY")
	filter.Selectivity = 0.1
	builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
		t,
		[]string{"category", "category", "tenant_id", "event_time", "event_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		[]*planpb.Expr{filter},
		&planpb.Stats{TableCnt: 800_000, Outcnt: 1_000, Selectivity: 0.00125, Cost: 350_000},
		map[int32]int{1: 1, 2: 1}, false,
	)
	node := builder.qry.Nodes[scanID]
	node.FilterList[0].GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
	require.True(t, builder.shouldSkipEncodedIndexOnlyScan(idxDef, node, colRefCnt, []int32{0, 0}, false))
}

func countPlanFunctionCalls(queryPlan *Plan, name string) int {
	var countExpr func(*planpb.Expr) int
	countExpr = func(expr *planpb.Expr) int {
		if expr == nil {
			return 0
		}
		count := 0
		switch impl := expr.Expr.(type) {
		case *planpb.Expr_F:
			if impl.F.Func != nil && impl.F.Func.ObjName == name {
				count++
			}
			for _, arg := range impl.F.Args {
				count += countExpr(arg)
			}
		case *planpb.Expr_List:
			for _, arg := range impl.List.List {
				count += countExpr(arg)
			}
		case *planpb.Expr_W:
			count += countExpr(impl.W.WindowFunc)
			for _, arg := range impl.W.PartitionBy {
				count += countExpr(arg)
			}
			for _, orderBy := range impl.W.OrderBy {
				count += countExpr(orderBy.Expr)
			}
		}
		return count
	}

	count := 0
	for _, node := range queryPlan.GetQuery().Nodes {
		for _, exprs := range [][]*planpb.Expr{
			node.ProjectList, node.OnList, node.FilterList, node.GroupBy,
			node.AggList, node.WinSpecList,
		} {
			for _, expr := range exprs {
				count += countExpr(expr)
			}
		}
		for _, orderBy := range node.OrderBy {
			count += countExpr(orderBy.Expr)
		}
	}
	return count
}

func countIndexFilterSerialExtractsFromPhysicalPK(queryPlan *Plan) int {
	count := 0
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		fn := expr.GetF()
		if fn == nil {
			return
		}
		if fn.Func != nil && fn.Func.ObjName == "serial_extract" && len(fn.Args) > 0 {
			if col := fn.Args[0].GetCol(); col != nil && col.ColPos == 1 {
				count++
			}
		}
		for _, arg := range fn.Args {
			visit(arg)
		}
	}
	for _, node := range queryPlan.GetQuery().Nodes {
		if node.IndexScanInfo.GetIsIndexScan() {
			for _, filter := range node.FilterList {
				visit(filter)
			}
		}
	}
	return count
}

func firstIndexLookupSerialArgCount(queryPlan *Plan) int {
	var serialArgCount func(*planpb.Expr) int
	serialArgCount = func(expr *planpb.Expr) int {
		if expr == nil || expr.GetF() == nil {
			return 0
		}
		fn := expr.GetF()
		for _, arg := range fn.Args {
			if serial := arg.GetF(); serial != nil && serial.Func != nil &&
				(serial.Func.ObjName == "serial" || serial.Func.ObjName == "serial_full") {
				return len(serial.Args)
			}
			if count := serialArgCount(arg); count > 0 {
				return count
			}
		}
		return 0
	}
	for _, node := range queryPlan.GetQuery().Nodes {
		if !node.IndexScanInfo.GetIsIndexScan() || len(node.FilterList) == 0 {
			continue
		}
		if count := serialArgCount(node.FilterList[0]); count > 0 {
			return count
		}
	}
	return 0
}

func TestEncodedIndexCostChargesUnpushableResidualFromPublicPlan(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(5)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	stats := ctx.statsByID[activity.TblId]
	stats.NdvMap["amount"] = 6_000
	stats.SizeMap["amount"] = uint64(stats.TableCnt) * 16
	stats.SizeMap["state"] = uint64(stats.TableCnt)
	stats.SizeMap[catalog.CPrimaryKeyColName] = uint64(stats.TableCnt)
	addCostActivityRegularIndex(t, ctx, "idx_state_narrow",
		[]string{"state", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, true)

	optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
	queryPlan, err := runOneStmt(optimizer, t,
		"select activity_id from cost_activity where state = 'READY' and amount = 7")
	require.NoError(t, err)
	require.Empty(t, findFirstIndexScanName(queryPlan))
	require.False(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexCostChargesCompoundPKExtractionFromPublicPlan(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(1_000)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	stats := ctx.statsByID[activity.TblId]
	stats.NdvMap["tenant_id"] = 1_000
	stats.SizeMap["tenant_id"] = uint64(stats.TableCnt) * 16
	stats.SizeMap["state"] = uint64(stats.TableCnt)
	stats.SizeMap[catalog.CPrimaryKeyColName] = uint64(stats.TableCnt)
	addCostActivityRegularIndex(t, ctx, "idx_state_narrow",
		[]string{"state", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, true)

	optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
	queryPlan, err := runOneStmt(optimizer, t,
		"select amount from cost_activity where state = 'READY' and tenant_id != 7")
	require.NoError(t, err)
	require.Equal(t, "idx_state_narrow", findFirstIndexScanName(queryPlan))
	require.True(t, planHasIndexJoin(queryPlan))
	require.Greater(t, countIndexFilterSerialExtractsFromPhysicalPK(queryPlan), 0,
		"the public witness must reach applyExtraFiltersOnIndex's compound-PK extraction")
}

func TestEncodedIndexCostDeduplicatesRepeatedLeadingPartFromPublicPlan(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(10)
	activity := ctx.tables["cost_activity"]
	activity.Indexes = nil
	ctx.statsByID[activity.TblId].NdvMap["amount"] = 100
	addCostActivityRegularIndex(t, ctx, "idx_state_repeated",
		[]string{
			"state", "state",
			"created_at", "tenant_id", "activity_id",
			catalog.CreateAlias(catalog.CPrimaryKeyColName),
		}, true)

	optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
	queryPlan, err := runOneStmt(optimizer, t,
		"select activity_id from cost_activity where state = 'READY' and amount = 7")
	require.NoError(t, err)
	require.Equal(t, "idx_state_repeated", findFirstIndexScanName(queryPlan))
	require.True(t, planHasIndexJoin(queryPlan))
	require.Equal(t, 2, firstIndexLookupSerialArgCount(queryPlan),
		"the public matcher must expose the duplicate leading filter identity")
}

func TestEncodedIndexCostIsReachableFromExistsPlan(t *testing.T) {
	const query = `
		select count(*), coalesce(sum(a.activity_id), 0)
		from cost_activity a
		where a.tenant_id between 7 and 23
		  and a.state = 'READY'
		  and exists (
			select 1 from cost_tags t
			where t.tenant_id = a.tenant_id
			  and t.activity_id = a.activity_id
			  and t.tag_id = 2
			  and t.weight >= 50.000
		  )`

	tests := []struct {
		name      string
		stateNDV  float64
		wantIndex bool
	}{
		{name: "broad state rejects encoded index", stateNDV: 2},
		{name: "selective state keeps encoded index", stateNDV: 1000, wantIndex: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := &encodedIndexPlanTestOptimizer{ctx: newEncodedExistsPlanTestContext(test.stateNDV)}
			queryPlan, err := runOneStmt(optimizer, t, query)
			require.NoError(t, err)
			if test.wantIndex {
				require.Equal(t, "idx_state_tenant_time_id", findFirstIndexScanName(queryPlan))
				require.GreaterOrEqual(t, countPlanFunctionCalls(queryPlan, "serial_extract"), 1)
			} else {
				require.Empty(t, findFirstIndexScanName(queryPlan))
				require.Zero(t, countPlanFunctionCalls(queryPlan, "serial_extract"))
			}
		})
	}
}

func TestEncodedIndexCostPreparedExistsStillRejectsBroadEncodedAccess(t *testing.T) {
	optimizer := &encodedIndexPlanTestOptimizer{ctx: newEncodedExistsPlanTestContext(2)}
	preparePlan, err := runOneStmt(optimizer, t, `
		prepare cost_exists_range from '
			select count(*), coalesce(sum(a.activity_id), 0)
			from cost_activity a
			where a.tenant_id between ? and ?
			  and a.state = ''READY''
			  and exists (
				select 1 from cost_tags t
				where t.tenant_id = a.tenant_id
				  and t.activity_id = a.activity_id
				  and t.tag_id = 2
				  and t.weight >= 50.000
			  )'`)
	require.NoError(t, err)
	queryPlan := resolveQueryPlan(preparePlan)
	require.Empty(t, findFirstIndexScanName(queryPlan),
		"unknown range bounds must not mask the stable broad encoded-access cost")
	require.Zero(t, countPlanFunctionCalls(queryPlan, "serial_extract"))
}

func TestEncodedIndexCostPreparedUnpushableResidualStillRejectsBroadBackfill(t *testing.T) {
	ctx := newEncodedExistsPlanTestContext(4)
	ctx.tables["cost_activity"].Indexes = nil
	addCostActivityRegularIndex(t, ctx, "idx_state_time_pk", []string{
		"state", "created_at", catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}, false)
	optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
	preparePlan, err := runOneStmt(optimizer, t, `
		prepare cost_unpushable_range from '
			select count(*)
			from cost_activity
			where state = ''READY''
			  and amount between ? and ?'`)
	require.NoError(t, err)
	queryPlan := resolveQueryPlan(preparePlan)
	require.Empty(t, findFirstIndexScanName(queryPlan),
		"an unavailable runtime residual cannot reduce the candidate backfill lower bound")
	require.False(t, planHasIndexJoin(queryPlan))
}

func TestEncodedIndexCostRejectsBroadBackfillSiblingFromPublicPlan(t *testing.T) {
	const query = `
		select a.state, count(*), coalesce(sum(a.amount), 0)
		from cost_activity a
		where a.tenant_id between 7 and 23
		  and a.state in ('READY', 'HOLD')
		  and exists (
			select 1 from cost_tags t
			where t.tenant_id = a.tenant_id
			  and t.activity_id = a.activity_id
			  and t.tag_id = 2
			  and t.weight >= 50.000
		  )
		group by a.state
		order by sum(a.amount) desc
		limit 20`

	for _, test := range []struct {
		name      string
		stateNDV  float64
		wantIndex bool
	}{
		{name: "broad predicate rejects covering and backfill siblings", stateNDV: 2},
		{name: "selective predicate keeps backfill index join", stateNDV: 1000, wantIndex: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			optimizer := &encodedIndexPlanTestOptimizer{ctx: newEncodedExistsPlanTestContext(test.stateNDV)}
			queryPlan, err := runOneStmt(optimizer, t, query)
			require.NoError(t, err)
			if test.wantIndex {
				require.Equal(t, "idx_state_tenant_time_id", findFirstIndexScanName(queryPlan))
				require.True(t, planHasIndexJoin(queryPlan))
			} else {
				require.Empty(t, findFirstIndexScanName(queryPlan))
				require.False(t, planHasIndexJoin(queryPlan))
				require.Zero(t, countPlanFunctionCalls(queryPlan, "serial_extract"))
			}
		})
	}
}

func TestEncodedIndexCostKeepsExactLeadingPaginationFromPublicPlan(t *testing.T) {
	for _, test := range []struct {
		name       string
		pagination string
		wantOffset uint64
	}{
		{name: "limit", pagination: "limit 1000"},
		{name: "limit with offset", pagination: "limit 1000 offset 500", wantOffset: 500},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := newEncodedExistsPlanTestContext(20)
			ctx.tables["cost_activity"].Indexes = nil
			addCostActivityRegularIndex(t, ctx, "idx_amount_activity", []string{
				"amount", "activity_id", catalog.CreateAlias(catalog.CPrimaryKeyColName),
			}, false)
			optimizer := &encodedIndexPlanTestOptimizer{ctx: ctx}
			queryPlan, err := runOneStmt(optimizer, t,
				"select activity_id from cost_activity where amount = 7 "+test.pagination)
			require.NoError(t, err)
			idxNode := findFirstIndexScanNode(queryPlan)
			require.NotNil(t, idxNode)
			require.Equal(t, "idx_amount_activity", idxNode.IndexScanInfo.IndexName)
			limit, ok := getLiteralUint64(idxNode.Limit)
			require.True(t, ok)
			require.Equal(t, uint64(1000), limit)
			if test.wantOffset == 0 {
				require.Nil(t, idxNode.Offset)
			} else {
				offset, ok := getLiteralUint64(idxNode.Offset)
				require.True(t, ok)
				require.Equal(t, test.wantOffset, offset)
			}
		})
	}
}

func TestEncodedIndexCostDoesNotCapByteStringPrefixResidualFromPublicPlan(t *testing.T) {
	optimizer := &encodedIndexPlanTestOptimizer{ctx: newEncodedExistsPlanTestContext(20)}
	queryPlan, err := runOneStmt(optimizer, t,
		"select activity_id from cost_activity where state = 'READY' limit 1000 offset 500")
	require.NoError(t, err)
	require.Empty(t, findFirstIndexScanName(queryPlan),
		"a byte-string prefix candidate needs an exact residual before pagination")
	require.False(t, planHasIndexJoin(queryPlan))
}

func TestTryIndexOnlyScanChargesResidualExtractionOnLeadingCandidates(t *testing.T) {
	parts := []string{
		"category",
		"tenant_id",
		"event_time",
		"event_id",
		catalog.CreateAlias(catalog.CPrimaryKeyColName),
	}
	tests := []struct {
		name       string
		leadingSel float64
		outcnt     float64
		wantIndex  bool
	}{
		{name: "narrow disjoint prefix wins", leadingSel: 0.01, outcnt: 0, wantIndex: true},
		{name: "broad correlated prefix loses despite small output", leadingSel: 0.2, outcnt: 800},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			categoryFilter := makeStringEqFilterExpr(0, 2, "category")
			categoryFilter.Selectivity = test.leadingSel
			eventFilter := makeRangeFilterExpr(0, 1, ">=", 1)
			eventFilter.Selectivity = 0.01
			builder, scanID, idxDef, colRefCnt := newEncodedIndexCostTestCase(
				t,
				parts,
				[]*planpb.Expr{categoryFilter, eventFilter},
				&planpb.Stats{TableCnt: 800_000, Outcnt: test.outcnt, Selectivity: test.outcnt / 800_000, Cost: 800_000},
				map[int32]int{0: 1, 2: 1},
				false,
			)
			bindTag := builder.qry.Nodes[scanID].BindingTags[0]
			for _, filter := range builder.qry.Nodes[scanID].FilterList {
				filter.GetF().Args[0].GetCol().RelPos = bindTag
			}

			idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], colRefCnt, map[[2]int32]*planpb.Expr{}, &Snapshot{})
			if test.wantIndex {
				require.NotEqual(t, int32(-1), idxNodeID)
			} else {
				require.Equal(t, int32(-1), idxNodeID)
			}
		})
	}
}

func TestSuspendScanProtection_RestoresExactCount(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 42

	builder.protectedScans[scanID] = 3
	restore := builder.suspendScanProtection(scanID)

	assert.False(t, builder.isScanProtected(scanID))

	restore()

	assert.Equal(t, 3, builder.protectedScans[scanID])
}

func TestSuspendScanProtection_NoExistingProtection(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 24

	restore := builder.suspendScanProtection(scanID)
	assert.False(t, builder.isScanProtected(scanID))

	restore()

	_, exists := builder.protectedScans[scanID]
	assert.False(t, exists)
}

func TestSuspendScanProtection_DoesNotDeleteNewProtection(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 88

	restore := builder.suspendScanProtection(scanID)
	builder.protectedScans[scanID] = 1

	restore()

	assert.Equal(t, 1, builder.protectedScans[scanID])
}

func TestSuspendScanProtection_PreservesNewProtectionAlongsideOriginal(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 89

	builder.protectedScans[scanID] = 3
	restore := builder.suspendScanProtection(scanID)
	builder.protectedScans[scanID] = 2

	restore()

	assert.Equal(t, 5, builder.protectedScans[scanID])
}

func TestWithSuspendedScanProtection_RestoresAfterPanic(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 64

	builder.protectedScans[scanID] = 2

	recovered := false
	func() {
		defer func() {
			if recover() != nil {
				recovered = true
			}
		}()

		builder.withSuspendedScanProtection(scanID, func() {
			assert.False(t, builder.isScanProtected(scanID))
			panic("boom")
		})
	}()

	assert.True(t, recovered)
	assert.Equal(t, 2, builder.protectedScans[scanID])
}

func TestFullTextJoinRewriteLeftChild(t *testing.T) {
	builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, true, false, false)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	joinNode := builder.qry.Nodes[joinID]
	require.NotEqual(t, leftScanID, joinNode.Children[0])
	require.Equal(t, rightScanID, joinNode.Children[1])
	require.Equal(t, planpb.Node_JOIN, builder.qry.Nodes[joinNode.Children[0]].NodeType)
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[0]))
	functionScan := collectFullTextFunctionScans(builder, joinNode.Children[0])[0]
	require.Nil(t, functionScan.TableDef.TblFunc.FulltextSourceRef)
	require.Nil(t, functionScan.TableDef.TblFunc.FulltextIndexRef)
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[leftScanID]))
	require.Len(t, joinNode.OnList, 1)
}

func TestFullTextJoinRewritePreservesPrimaryKeyCharset(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, false)
	builder.qry.Nodes[leftScanID].TableDef.Cols[0].Typ.Charset = uint32(types.CharsetUTF8MB4Bin)

	_, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	functionScans := collectFullTextFunctionScans(builder, builder.qry.Nodes[joinID].Children[0])
	require.Len(t, functionScans, 1)
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), functionScans[0].TableDef.Cols[0].Typ.Charset)
}

func TestFullTextJoinRewriteRightChild(t *testing.T) {
	builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, false, true, false)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	joinNode := builder.qry.Nodes[joinID]
	require.Equal(t, leftScanID, joinNode.Children[0])
	require.NotEqual(t, rightScanID, joinNode.Children[1])
	require.Equal(t, planpb.Node_JOIN, builder.qry.Nodes[joinNode.Children[1]].NodeType)
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[1]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[rightScanID]))
	require.Len(t, joinNode.OnList, 1)
}

func TestFullTextSemiJoinRewriteRightChild(t *testing.T) {
	builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, false, true, false)
	joinNode := builder.qry.Nodes[joinID]
	joinNode.JoinType = planpb.Node_SEMI

	newID, err := builder.applyIndices(joinID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.Equal(t, leftScanID, joinNode.Children[0])
	require.NotEqual(t, rightScanID, joinNode.Children[1])
	require.Equal(t, planpb.Node_JOIN, builder.qry.Nodes[joinNode.Children[1]].NodeType)
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[1]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[rightScanID]))
	require.Len(t, joinNode.OnList, 1)
}

func TestFullTextJoinRewriteFallsBackToScanContextWhenJoinContextIsNil(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, false)
	builder.ctxByNode[joinID] = nil

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.NotEqual(t, leftScanID, builder.qry.Nodes[joinID].Children[0])
	require.Equal(t, 1, countFullTextFunctionScans(builder, builder.qry.Nodes[joinID].Children[0]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[leftScanID]))
}

func TestFullTextJoinRewriteCarriesPublisherReferences(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, false)
	scan := builder.qry.Nodes[leftScanID]
	scan.ObjRef.SchemaName = "pub`db"
	scan.ObjRef.ObjName = "source`table"
	scan.ObjRef.SubscriptionName = "subscriber_alias"
	scan.ObjRef.PubInfo = &planpb.PubInfo{TenantId: 42}

	indexName := scan.TableDef.Indexes[0].IndexTableName
	mockCtx := builder.compCtx.(*fullTextJoinMockCompilerContext)
	indexRef := mockCtx.objects[strings.ToLower(indexName)]
	indexRef.SchemaName = scan.ObjRef.SchemaName
	indexRef.ObjName = "index`table"
	indexRef.SubscriptionName = scan.ObjRef.SubscriptionName
	indexRef.PubInfo = &planpb.PubInfo{TenantId: 42}

	_, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	functionScans := collectFullTextFunctionScans(builder, builder.qry.Nodes[joinID].Children[0])
	require.Len(t, functionScans, 1)
	functionScan := functionScans[0]
	require.Equal(t, "`pub``db`.`source``table`", functionScan.TblFuncExprList[0].GetLit().GetSval())
	require.Equal(t, "`pub``db`.`index``table`", functionScan.TblFuncExprList[1].GetLit().GetSval())
	require.Equal(t, scan.ObjRef, functionScan.TableDef.TblFunc.FulltextSourceRef)
	require.Equal(t, indexRef, functionScan.TableDef.TblFunc.FulltextIndexRef)
	require.NotSame(t, scan.ObjRef, functionScan.TableDef.TblFunc.FulltextSourceRef)
	require.NotSame(t, indexRef, functionScan.TableDef.TblFunc.FulltextIndexRef)
}

func TestFullTextJoinRewriteBothChildren(t *testing.T) {
	builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, true, true, false)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	joinNode := builder.qry.Nodes[joinID]
	require.NotEqual(t, leftScanID, joinNode.Children[0])
	require.NotEqual(t, rightScanID, joinNode.Children[1])
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[0]))
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[1]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[leftScanID]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[rightScanID]))
}

func TestFullTextJoinRewriteSkipsOuterJoins(t *testing.T) {
	tests := []struct {
		name          string
		joinType      planpb.Node_JoinType
		leftFullText  bool
		rightFullText bool
	}{
		{
			name:         "left join preserved left child",
			joinType:     planpb.Node_LEFT,
			leftFullText: true,
		},
		{
			name:          "right join preserved right child",
			joinType:      planpb.Node_RIGHT,
			rightFullText: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, tt.leftFullText, tt.rightFullText, false)
			joinNode := builder.qry.Nodes[joinID]
			joinNode.JoinType = tt.joinType

			newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			require.Equal(t, joinID, newID)
			require.Equal(t, leftScanID, joinNode.Children[0])
			require.Equal(t, rightScanID, joinNode.Children[1])
			require.Equal(t, 0, countFullTextFunctionScans(builder, joinID))

			if tt.leftFullText {
				require.True(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[leftScanID]))
			}
			if tt.rightFullText {
				require.True(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[rightScanID]))
			}
		})
	}
}

func TestFullTextJoinRewritePreservesNonFullTextFilter(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, true)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	leftScan := builder.qry.Nodes[leftScanID]
	require.Len(t, leftScan.FilterList, 1)
	require.Equal(t, "=", leftScan.FilterList[0].GetF().Func.ObjName)
	require.False(t, nodeHasFullTextMatchFilter(leftScan))
	require.Equal(t, 1, countFullTextFunctionScans(builder, builder.qry.Nodes[joinID].Children[0]))
}

func TestFullTextCandidateLimitIncludesOffset(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, false)
	scan := builder.qry.Nodes[leftScanID]
	scan.Limit = makePlan2Uint64ConstExprWithType(10)
	scan.Offset = makePlan2Uint64ConstExprWithType(5)

	newID, changed, err := builder.applyFullTextFiltersForScanInJoin(
		joinID,
		scan,
		map[[2]int32]int{},
		map[[2]int32]*planpb.Expr{},
	)
	require.NoError(t, err)
	require.True(t, changed)
	functions := collectFullTextFunctionScans(builder, newID)
	require.Len(t, functions, 1)
	require.Equal(t, uint64(15), functions[0].Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(10), builder.qry.Nodes[newID].Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(5), builder.qry.Nodes[newID].Offset.GetLit().GetU64Val())
	require.Nil(t, scan.Limit)
	require.Nil(t, scan.Offset)
}

func TestFullTextCandidateLimitWithoutResidualFilterKeepsDynamicLimit(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, false)
	scan := builder.qry.Nodes[leftScanID]
	scan.Limit = &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}

	newID, changed, err := builder.applyFullTextFiltersForScanInJoin(
		joinID,
		scan,
		map[[2]int32]int{},
		map[[2]int32]*planpb.Expr{},
	)
	require.NoError(t, err)
	require.True(t, changed)
	functions := collectFullTextFunctionScans(builder, newID)
	require.Len(t, functions, 1)
	require.NotNil(t, functions[0].Limit.GetP())
	require.Equal(t, int32(0), functions[0].Limit.GetP().Pos)
}

func TestFullTextCandidateLimitSQLCalcFoundRowsKeepsCompleteStream(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, false)
	builder.sqlCalcFoundRows = true
	scan := builder.qry.Nodes[leftScanID]
	scan.Limit = makePlan2Uint64ConstExprWithType(10)
	scan.Offset = makePlan2Uint64ConstExprWithType(5)

	newID, changed, err := builder.applyFullTextFiltersForScanInJoin(
		joinID,
		scan,
		map[[2]int32]int{},
		map[[2]int32]*planpb.Expr{},
	)
	require.NoError(t, err)
	require.True(t, changed)
	functions := collectFullTextFunctionScans(builder, newID)
	require.Len(t, functions, 1)
	require.Nil(t, functions[0].Limit,
		"the full-text TVF must not truncate candidates before FOUND_ROWS counting")
	require.Equal(t, uint64(10), builder.qry.Nodes[newID].Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(5), builder.qry.Nodes[newID].Offset.GetLit().GetU64Val())
	require.Nil(t, scan.Limit)
	require.Nil(t, scan.Offset)
}

func TestFullTextCandidateLimitWithResidualFilterRequiresExactPrefilter(t *testing.T) {
	tests := []struct {
		name            string
		pkType          types.T
		pushdown        int8
		dynamicLimit    bool
		preparedPattern bool
		mode            tree.FullTextSearchType
		classicIndex    bool
		parser          string
		pattern         string
		wantCandidateK  bool
	}{
		{name: "exact int8 prefilter", pkType: types.T_int8, pushdown: 1, pattern: "+hello +world", wantCandidateK: true},
		{name: "exact int16 prefilter", pkType: types.T_int16, pushdown: 1, pattern: "+hello +world", wantCandidateK: true},
		{name: "exact int32 prefilter", pkType: types.T_int32, pushdown: 1, pattern: "+hello +world", wantCandidateK: true},
		{name: "exact int64 prefilter", pkType: types.T_int64, pushdown: 1, pattern: "+hello +world", wantCandidateK: true},
		{name: "exact uint8 prefilter", pkType: types.T_uint8, pushdown: 1, pattern: "+hello +world", wantCandidateK: true},
		{name: "exact uint16 prefilter", pkType: types.T_uint16, pushdown: 1, pattern: "+hello +world", wantCandidateK: true},
		{name: "exact uint32 prefilter", pkType: types.T_uint32, pushdown: 1, pattern: "+hello +world", wantCandidateK: true},
		{name: "exact uint64 prefilter", pkType: types.T_uint64, pushdown: 1, pattern: "+hello +world", wantCandidateK: true},
		{name: "bit is not an exact membership type", pkType: types.T_bit, pushdown: 1, pattern: "+hello +world"},
		{name: "approximate varchar prefilter", pkType: types.T_varchar, pushdown: 1, pattern: "+hello +world"},
		{name: "uuid prefilter", pkType: types.T_uuid, pushdown: 1, pattern: "+hello +world"},
		{name: "pushdown disabled", pkType: types.T_int64, pattern: "+hello +world"},
		{name: "prepared limit", pkType: types.T_int64, pushdown: 1, dynamicLimit: true, pattern: "+hello +world"},
		{name: "prepared pattern", pkType: types.T_int64, pushdown: 1, preparedPattern: true},
		{name: "natural mode", pkType: types.T_int64, pushdown: 1, mode: tree.FULLTEXT_NL, pattern: "hello world"},
		{name: "should clause", pkType: types.T_int64, pushdown: 1, pattern: "+hello world"},
		{name: "explicit phrase", pkType: types.T_int64, pushdown: 1, pattern: `+"hello world"`},
		{name: "prefix", pkType: types.T_int64, pushdown: 1, pattern: "+hello*"},
		{name: "group", pkType: types.T_int64, pushdown: 1, pattern: "+(hello world)"},
		{name: "must not", pkType: types.T_int64, pushdown: 1, pattern: "+hello -world"},
		{name: "adjust", pkType: types.T_int64, pushdown: 1, pattern: "+hello ~world"},
		{name: "default cjk becomes phrase", pkType: types.T_int64, pushdown: 1, pattern: "+中"},
		{name: "ngram cjk becomes phrase", pkType: types.T_int64, pushdown: 1, parser: fulltext2.ParserNgram, pattern: "+中文"},
		{name: "json value atomic term", pkType: types.T_int64, pushdown: 1, parser: fulltext2.ParserJSONValue, pattern: "+json.value", wantCandidateK: true},
		{name: "classic index", pkType: types.T_int64, pushdown: 1, classicIndex: true, pattern: "+hello +world"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, true)
			mockCtx := builder.compCtx.(*fullTextJoinMockCompilerContext)
			mockCtx.fulltextBloomFilterPushdown = tc.pushdown
			scan := builder.qry.Nodes[leftScanID]
			if !tc.classicIndex {
				convertFullTextJoinTestToFulltext2(builder, scan)
				if tc.parser != "" {
					scan.TableDef.Indexes[0].IndexAlgoParams = fmt.Sprintf(`{"parser":%q}`, tc.parser)
				}
			}
			matchFn := scan.FilterList[0].GetF()
			if tc.preparedPattern {
				matchFn.Args[0] = &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			} else {
				matchFn.Args[0] = makePlan2StringConstExprWithType(tc.pattern, false)
			}
			mode := tc.mode
			if mode == 0 {
				mode = tree.FULLTEXT_BOOLEAN
			}
			matchFn.Args[1] = makePlan2Int64ConstExprWithType(int64(mode))
			pkType := planpb.Type{Id: int32(tc.pkType)}
			scan.TableDef.Cols[0].Typ = pkType
			indexTable := mockCtx.tables[strings.ToLower(scan.TableDef.Indexes[0].IndexTableName)]
			indexTable.Cols[1].Typ = pkType
			if tc.dynamicLimit {
				scan.Limit = &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			} else {
				scan.Limit = makePlan2Uint64ConstExprWithType(10)
			}
			scan.Offset = makePlan2Uint64ConstExprWithType(5)

			newID, changed, err := builder.applyFullTextFiltersForScanInJoin(
				joinID,
				scan,
				map[[2]int32]int{},
				map[[2]int32]*planpb.Expr{},
			)
			require.NoError(t, err)
			require.True(t, changed)
			functions := collectFullTextFunctionScans(builder, newID)
			require.Len(t, functions, 1)
			if tc.wantCandidateK {
				require.Equal(t, uint64(15), functions[0].Limit.GetLit().GetU64Val())
			} else {
				require.Nil(t, functions[0].Limit)
			}
		})
	}
}

func TestFullTextCandidateLimitRejectsVolatileResidualFilter(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, true)
	mockCtx := builder.compCtx.(*fullTextJoinMockCompilerContext)
	mockCtx.fulltextBloomFilterPushdown = 1
	scan := builder.qry.Nodes[leftScanID]
	convertFullTextJoinTestToFulltext2(builder, scan)

	pkType := planpb.Type{Id: int32(types.T_int64)}
	scan.TableDef.Cols[0].Typ = pkType
	indexTable := mockCtx.tables[strings.ToLower(scan.TableDef.Indexes[0].IndexTableName)]
	indexTable.Cols[1].Typ = pkType
	matchFn := scan.FilterList[0].GetF()
	matchFn.Args[0] = makePlan2StringConstExprWithType("+needle +world", false)
	matchFn.Args[1] = makePlan2Int64ConstExprWithType(int64(tree.FULLTEXT_BOOLEAN))
	scan.FilterList = append(scan.FilterList,
		makeVolatileJoinFilter(t, mockCtx.MockCompilerContext, nil))
	scan.Limit = makePlan2Uint64ConstExprWithType(2)
	scan.Offset = makePlan2Uint64ConstExprWithType(1)

	newID, changed, err := builder.applyFullTextFiltersForScanInJoin(
		joinID,
		scan,
		map[[2]int32]int{},
		map[[2]int32]*planpb.Expr{},
	)
	require.NoError(t, err)
	require.True(t, changed)
	functions := collectFullTextFunctionScans(builder, newID)
	require.Len(t, functions, 1)
	require.Nil(t, functions[0].Limit,
		"a volatile residual must not receive a filter-dependent candidate bound")

	var volatileScanNodes int
	var visit func(int32)
	visit = func(nodeID int32) {
		node := builder.qry.Nodes[nodeID]
		if node == nil {
			return
		}
		if node.NodeType == planpb.Node_TABLE_SCAN {
			for _, filter := range node.FilterList {
				if containsVolatileFunction(filter) {
					volatileScanNodes++
					break
				}
			}
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	visit(newID)
	require.Equal(t, 1, volatileScanNodes,
		"the volatile residual must be evaluated only by the final scan")
}

func convertFullTextJoinTestToFulltext2(builder *QueryBuilder, scan *planpb.Node) {
	logical := scan.TableDef.Indexes[0]
	store := *logical
	store.IndexAlgo = tree.INDEX_TYPE_FULLTEXT2.ToString()
	store.IndexAlgoParams = `{"parser":"default"}`
	store.IndexAlgoTableType = catalog.FullText2Index_TblType_Storage
	store.IndexTableName = logical.IndexTableName + "_store"
	meta := store
	meta.IndexAlgoTableType = catalog.FullText2Index_TblType_Metadata
	meta.IndexTableName = logical.IndexTableName + "_meta"
	scan.TableDef.Indexes = []*planpb.IndexDef{&store, &meta}
	registerFullTextJoinRegularIndexTable(builder, store.IndexTableName)
}

func TestFullTextDoesNotLimitIndependentIntersectionInputs(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, false)
	scan := builder.qry.Nodes[leftScanID]
	scan.FilterList = append(scan.FilterList, DeepCopyExpr(scan.FilterList[0]))
	scan.Limit = makePlan2Uint64ConstExprWithType(10)
	scan.Offset = makePlan2Uint64ConstExprWithType(5)

	newID, changed, err := builder.applyFullTextFiltersForScanInJoin(
		joinID,
		scan,
		map[[2]int32]int{},
		map[[2]int32]*planpb.Expr{},
	)
	require.NoError(t, err)
	require.True(t, changed)
	functions := collectFullTextFunctionScans(builder, newID)
	require.Len(t, functions, 2)
	for _, functionNode := range functions {
		require.Nil(t, functionNode.Limit)
	}
}

func TestFullTextScanProtectionSkipsRegularIndexRule(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, true)
	leftScan := builder.qry.Nodes[leftScanID]
	leftScan.TableDef.Indexes = append(leftScan.TableDef.Indexes, &planpb.IndexDef{
		IndexName:      "idx_base_id",
		IndexTableName: "__mo_idx_base_id",
		Parts:          []string{"base_id", "id"},
		TableExist:     true,
	})
	registerFullTextJoinRegularIndexTable(builder, "__mo_idx_base_id")

	got := builder.applyIndicesForFilters(leftScanID, leftScan, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.Equal(t, leftScanID, got)
	require.True(t, builder.scanHasMatchedFullTextFilter(leftScan))

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.False(t, nodeHasFullTextMatchFilter(leftScan))
	require.Len(t, leftScan.FilterList, 1)
}

func TestRegularIndexRuleSkipsIrregularIndexes(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	nodeID := int32(12)
	node := &planpb.Node{
		NodeId:      nodeID,
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name: "t",
			Name2ColIndex: map[string]int32{
				"id":     0,
				"status": 1,
			},
			Cols: []*planpb.ColDef{
				{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "status", Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
			},
			Pkey: &planpb.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
			Indexes: []*planpb.IndexDef{
				{
					IndexName:      "idx_master_status",
					IndexAlgo:      catalog.MOIndexMasterAlgo.ToString(),
					IndexTableName: "__mo_index_master_status",
					Parts:          []string{"status", "id"},
					TableExist:     true,
				},
				{
					IndexName:      "idx_ivf_status",
					IndexAlgo:      catalog.MoIndexIvfFlatAlgo.ToString(),
					IndexTableName: "__mo_index_ivf_status",
					Parts:          []string{"status", "id"},
					TableExist:     true,
				},
			},
		},
		Stats: &planpb.Stats{TableCnt: 10, Outcnt: 1, Selectivity: 0.1},
		FilterList: []*planpb.Expr{
			makeStringEqFilterExpr(bindTag, 1, "active"),
		},
	}

	got := builder.applyIndicesForFiltersRegularIndex(nodeID, node, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})

	require.Equal(t, nodeID, got)
	require.Empty(t, builder.qry.Nodes)
}

func TestApplyIndicesForJoinsSkipsIrregularIndexes(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()

	leftDef := &planpb.TableDef{
		Name: "left_t",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "status", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
		Name2ColIndex: map[string]int32{
			"id":     0,
			"status": 1,
		},
		Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Indexes: []*planpb.IndexDef{
			{
				IndexName:      "idx_master_status",
				IndexAlgo:      catalog.MOIndexMasterAlgo.ToString(),
				IndexTableName: "__mo_master_status",
				Parts:          []string{"status", "id"},
				TableExist:     true,
			},
			{
				IndexName:      "idx_fulltext_status",
				IndexAlgo:      catalog.MOIndexFullTextAlgo.ToString(),
				IndexTableName: "__mo_fulltext_status",
				Parts:          []string{"status", "id"},
				TableExist:     true,
			},
			{
				IndexName:      "idx_vector_status",
				IndexAlgo:      catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexTableName: "__mo_vector_status",
				Parts:          []string{"status", "id"},
				TableExist:     true,
			},
			{
				IndexName:      "idx_spatial_status",
				IndexAlgo:      catalog.MoIndexRTreeAlgo.ToString(),
				IndexTableName: "__mo_spatial_status",
				Parts:          []string{"status", "id"},
				TableExist:     true,
			},
		},
	}
	rightDef := &planpb.TableDef{
		Name: "right_t",
		Cols: []*planpb.ColDef{
			{Name: "status", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
		Name2ColIndex: map[string]int32{"status": 0},
	}

	leftScanID := builder.appendNode(makeJoinIndexTestScan(leftDef, leftTag), ctx)
	rightScanID := builder.appendNode(makeJoinIndexTestScan(rightDef, rightTag), ctx)
	joinCond := ftjMakeEqExpr(t, ftjColExpr(leftDef, leftTag, 1), ftjColExpr(rightDef, rightTag, 0))
	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		Children: []int32{leftScanID, rightScanID},
		JoinType: planpb.Node_INNER,
		OnList:   []*planpb.Expr{joinCond},
	}, ctx)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	joinNode := builder.qry.Nodes[joinID]
	require.Equal(t, leftScanID, joinNode.Children[0])
	require.Equal(t, rightScanID, joinNode.Children[1])
	require.Empty(t, joinNode.RuntimeFilterBuildList)
	require.Len(t, builder.qry.Nodes, 3)
}

func TestFindMatchFullTextIndexRequiresScanBindingAndConstantMode(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ftDef := makeFullTextJoinTestTableDef("ft", true)
	ftDef.Indexes[0].Visible = false
	ftTag := builder.genNewBindTag()
	baseTag := builder.genNewBindTag()
	scan := makeFullTextJoinTestScan(ftDef, ftTag, nil)

	matched := builder.findMatchFullTextIndex(makeFullTextMatchExpr("hello", 0, ftDef, ftTag, []int32{2, 3}).GetF(), scan)
	require.NotNil(t, matched)

	crossTableExpr := makeFullTextMatchExpr("hello", 0, ftDef, ftTag, []int32{2})
	crossTableExpr.GetF().Args = append(crossTableExpr.GetF().Args, &planpb.Expr{
		Typ: ftDef.Cols[3].Typ,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: baseTag,
			ColPos: 3,
			Name:   "body",
		}},
	})
	require.Nil(t, builder.findMatchFullTextIndex(crossTableExpr.GetF(), scan))

	dynamicPatternExpr := makeFullTextMatchExpr("hello", 0, ftDef, ftTag, []int32{2, 3})
	textTyp := types.T_text.ToType()
	dynamicPatternExpr.GetF().Args[0] = &planpb.Expr{
		Typ:  makePlan2Type(&textTyp),
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	require.NotNil(t, builder.findMatchFullTextIndex(dynamicPatternExpr.GetF(), scan))

	dynamicModeExpr := makeFullTextMatchExpr("hello", 0, ftDef, ftTag, []int32{2, 3})
	int64Typ := types.T_int64.ToType()
	dynamicModeExpr.GetF().Args[1] = &planpb.Expr{
		Typ:  makePlan2Type(&int64Typ),
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}},
	}
	require.Nil(t, builder.findMatchFullTextIndex(dynamicModeExpr.GetF(), scan))
}

func buildFullTextJoinRewriteTestPlan(t *testing.T, leftFullText, rightFullText, leftExtraFilter bool) (*QueryBuilder, int32, int32, int32) {
	t.Helper()

	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()
	leftDef := makeFullTextJoinTestTableDef("ft_left", leftFullText)
	rightDef := makeFullTextJoinTestTableDef("ft_right", rightFullText)
	if leftFullText {
		registerFullTextJoinRegularIndexTable(builder, leftDef.Indexes[0].IndexTableName)
	}
	if rightFullText {
		registerFullTextJoinRegularIndexTable(builder, rightDef.Indexes[0].IndexTableName)
	}

	var leftFilters []*planpb.Expr
	if leftFullText {
		leftFilters = append(leftFilters, makeFullTextMatchExpr("hello", 0, leftDef, leftTag, []int32{2, 3}))
	}
	if leftExtraFilter {
		leftFilters = append(leftFilters, ftjMakeEqExpr(t, ftjColExpr(leftDef, leftTag, 1), makePlan2StringConstExprWithType("b1", false)))
	}

	var rightFilters []*planpb.Expr
	if rightFullText {
		rightFilters = append(rightFilters, makeFullTextMatchExpr("hello", 0, rightDef, rightTag, []int32{2, 3}))
	}

	leftScanID := builder.appendNode(makeFullTextJoinTestScan(leftDef, leftTag, leftFilters), ctx)
	rightScanID := builder.appendNode(makeFullTextJoinTestScan(rightDef, rightTag, rightFilters), ctx)
	joinCond := ftjMakeEqExpr(t, ftjColExpr(leftDef, leftTag, 1), ftjColExpr(rightDef, rightTag, 0))
	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		Children: []int32{leftScanID, rightScanID},
		JoinType: planpb.Node_INNER,
		OnList:   []*planpb.Expr{joinCond},
	}, ctx)

	return builder, joinID, leftScanID, rightScanID
}

type fullTextJoinMockCompilerContext struct {
	*MockCompilerContext
	fulltextBloomFilterPushdown int8
}

func newFullTextJoinMockCompilerContext() *fullTextJoinMockCompilerContext {
	return &fullTextJoinMockCompilerContext{MockCompilerContext: NewMockCompilerContext(true)}
}

func (m *fullTextJoinMockCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	if varName == "ft_relevancy_algorithm" {
		return "", nil
	}
	if varName == "fulltext_bloom_filter_pushdown" {
		return m.fulltextBloomFilterPushdown, nil
	}
	return m.MockCompilerContext.ResolveVariable(varName, isSystemVar, isGlobalVar)
}

func (m *fullTextJoinMockCompilerContext) GetProcess() *process.Process {
	proc := m.MockCompilerContext.GetProcess()
	proc.SetResolveVariableFunc(m.ResolveVariable)
	return proc
}

func registerFullTextJoinRegularIndexTable(builder *QueryBuilder, indexTableName string) {
	mockCtx := builder.compCtx.(*fullTextJoinMockCompilerContext)
	key := strings.ToLower(indexTableName)
	mockCtx.objects[key] = &planpb.ObjectRef{
		SchemaName: "test",
		ObjName:    indexTableName,
	}
	mockCtx.tables[key] = &planpb.TableDef{
		Name: indexTableName,
		Cols: []*planpb.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
			{Name: catalog.IndexTablePrimaryColName, Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
		},
	}
}

func makeFullTextJoinTestTableDef(name string, withFullTextIndex bool) *planpb.TableDef {
	tableDef := &planpb.TableDef{
		Name: name,
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
			{Name: "base_id", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
			{Name: "title", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 512}},
			{Name: "body", Typ: planpb.Type{Id: int32(types.T_text)}},
		},
		Name2ColIndex: map[string]int32{
			"id":      0,
			"base_id": 1,
			"title":   2,
			"body":    3,
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	if withFullTextIndex {
		tableDef.Indexes = append(tableDef.Indexes, &planpb.IndexDef{
			IndexName:      "ft_idx_" + name,
			IndexAlgo:      catalog.MOIndexFullTextAlgo.ToString(),
			IndexTableName: "__mo_fts_idx_" + name,
			Parts:          []string{"title", "body"},
			TableExist:     true,
		})
	}
	return tableDef
}

func makeFullTextJoinTestScan(tableDef *planpb.TableDef, tag int32, filters []*planpb.Expr) *planpb.Node {
	return &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: tableDef.Name},
		BindingTags: []int32{tag},
		FilterList:  filters,
		Stats: &planpb.Stats{
			TableCnt:    1000,
			Outcnt:      100,
			Selectivity: 0.1,
			Cost:        1000,
		},
	}
}

func makeJoinIndexTestScan(tableDef *planpb.TableDef, tag int32) *planpb.Node {
	return &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: tableDef.Name},
		BindingTags: []int32{tag},
		Stats: &planpb.Stats{
			TableCnt:    1000,
			Outcnt:      100,
			Selectivity: 0.1,
			Cost:        1000,
		},
	}
}

func makeFullTextMatchExpr(pattern string, mode int64, tableDef *planpb.TableDef, tag int32, colPositions []int32) *planpb.Expr {
	args := []*planpb.Expr{
		makePlan2StringConstExprWithType(pattern, false),
		makePlan2Int64ConstExprWithType(mode),
	}
	for _, pos := range colPositions {
		args = append(args, ftjColExpr(tableDef, tag, pos))
	}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "fulltext_match"},
			Args: args,
		}},
	}
}

func ftjColExpr(tableDef *planpb.TableDef, tag, pos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ: tableDef.Cols[pos].Typ,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: tag,
			ColPos: pos,
			Name:   tableDef.Cols[pos].Name,
		}},
	}
}

func ftjMakeEqExpr(t *testing.T, left, right *planpb.Expr) *planpb.Expr {
	t.Helper()

	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{left, right})
	require.NoError(t, err)
	return expr
}

func countFullTextFunctionScans(builder *QueryBuilder, nodeID int32) int {
	return len(collectFullTextFunctionScans(builder, nodeID))
}

func collectFullTextFunctionScans(builder *QueryBuilder, nodeID int32) []*planpb.Node {
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return nil
	}

	var nodes []*planpb.Node
	if node.NodeType == planpb.Node_FUNCTION_SCAN &&
		node.TableDef != nil && node.TableDef.TblFunc != nil {
		name := node.TableDef.TblFunc.Name
		if name == fulltext_index_scan_func_name || name == fulltext2_search_func_name {
			nodes = append(nodes, node)
		}
	}
	for _, childID := range node.Children {
		nodes = append(nodes, collectFullTextFunctionScans(builder, childID)...)
	}
	return nodes
}

func nodeHasFullTextMatchFilter(node *planpb.Node) bool {
	for _, expr := range node.FilterList {
		fn := expr.GetF()
		if fn != nil && fn.Func.ObjName == "fulltext_match" {
			return true
		}
	}
	return false
}
func TestPositiveLiteralLimitRejectsReaderOverflow(t *testing.T) {
	require.True(t, isPositiveLiteralLimit(makePlan2Uint64ConstExprWithType(1)))
	require.False(t, isPositiveLiteralLimit(makePlan2Uint64ConstExprWithType(0)))
	require.False(t, isPositiveLiteralLimit(makePlan2Uint64ConstExprWithType(maxVectorIndexTopPushdownLimit+1)))
}
func makeTestRegularIndexPrefixEq(t *testing.T, numArgs int) *planpb.Expr {
	return makeTestRegularIndexPrefixEqWithSerialFunc(t, numArgs, "serial")
}

func makeTestRegularIndexPrefixEqWithSerialFunc(t *testing.T, numArgs int, serialFunc string) *planpb.Expr {
	t.Helper()
	args := make([]*planpb.Expr, 0, numArgs)
	for i := 0; i < numArgs; i++ {
		args = append(args, &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_int32)},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{
					Value: &planpb.Literal_I32Val{I32Val: int32(i + 1)},
				},
			},
		})
	}
	serialExpr, err := BindFuncExprImplByPlanExpr(context.Background(), serialFunc, args)
	require.NoError(t, err)
	prefixExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "prefix_eq", []*planpb.Expr{
		GetColExpr(planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, 100, 0),
		serialExpr,
	})
	require.NoError(t, err)
	return prefixExpr
}

func makeTestRegularIndexPKLessThan(t *testing.T, value int64) *planpb.Expr {
	return makeTestRegularIndexPKRange(t, "<", 100, 1, value, false)
}

func makeTestRegularIndexPKRange(t *testing.T, op string, tag, colPos int32, value int64, constFirst bool) *planpb.Expr {
	t.Helper()
	colExpr := GetColExpr(planpb.Type{Id: int32(types.T_int64)}, tag, colPos)
	valueExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{
				Value: &planpb.Literal_I64Val{I64Val: value},
			},
		},
	}
	args := []*planpb.Expr{colExpr, valueExpr}
	if constFirst {
		args[0], args[1] = args[1], args[0]
	}
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), op, args)
	require.NoError(t, err)
	return expr
}

func requireTestRegularIndexCursorRange(t *testing.T, expr *planpb.Expr, numKeyParts int, op string) *planpb.Expr {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "prefix_in_range", fn.Func.ObjName)
	require.Len(t, fn.Args, 4)

	leftSerial := fn.Args[1].GetF()
	rightSerial := fn.Args[2].GetF()
	require.NotNil(t, leftSerial)
	require.NotNil(t, rightSerial)
	require.Equal(t, "serial", leftSerial.Func.ObjName)
	require.Equal(t, "serial", rightSerial.Func.ObjName)

	var prefixArgs, fullArgs []*planpb.Expr
	switch op {
	case "<", "<=":
		prefixArgs, fullArgs = leftSerial.Args, rightSerial.Args
	case ">", ">=":
		prefixArgs, fullArgs = rightSerial.Args, leftSerial.Args
	default:
		t.Fatalf("unsupported cursor operator %q", op)
	}
	require.Len(t, prefixArgs, numKeyParts)
	require.Len(t, fullArgs, numKeyParts+1)
	for i := range prefixArgs {
		require.True(t, reflect.DeepEqual(prefixArgs[i], fullArgs[i]))
	}

	wantFlag := uint32(0)
	if op == "<" {
		wantFlag = 2
	} else if op == ">" {
		wantFlag = 1
	}
	require.Equal(t, wantFlag, fn.Args[3].GetLit().GetU8Val())
	return fullArgs[numKeyParts]
}

func makeTestRegularIndexProjectBuilder(
	t *testing.T,
	prefixArgCount int,
	projectExpr *planpb.Expr,
	sortFlag planpb.OrderBySpec_OrderByFlag,
) (*QueryBuilder, int32) {
	t.Helper()

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.nameByColRef[[2]int32{200, 0}] = "id"

	scanNode := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		NodeId:   0,
		TableDef: &planpb.TableDef{
			Cols: []*planpb.ColDef{
				{
					Name: catalog.IndexTableIndexColName,
					Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
				},
				{
					Name: catalog.IndexTablePrimaryColName,
					Typ:  planpb.Type{Id: int32(types.T_int64)},
				},
			},
			Indexes: []*planpb.IndexDef{{IndexName: "idx_user_active"}},
		},
		BindingTags: []int32{100},
		FilterList:  []*planpb.Expr{makeTestRegularIndexPrefixEq(t, prefixArgCount)},
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan:    true,
			IndexName:      "idx_user_active",
			BelongToTable:  "events",
			Parts:          []string{"user_id", "is_active", "id"},
			IsUnique:       false,
			IndexTableName: "__mo_index_secondary_idx_user_active",
		},
	}

	sortProjectNode := &planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		NodeId:      1,
		BindingTags: []int32{200},
		Children:    []int32{0},
		ProjectList: []*planpb.Expr{projectExpr},
	}

	sortNode := &planpb.Node{
		NodeType: planpb.Node_SORT,
		NodeId:   2,
		Children: []int32{1},
		OrderBy: []*planpb.OrderBySpec{
			{
				Expr: GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 200, 0),
				Flag: sortFlag,
			},
		},
		Limit: &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{
					Value: &planpb.Literal_U64Val{U64Val: 20},
				},
			},
		},
	}

	projNode := &planpb.Node{
		NodeType: planpb.Node_PROJECT,
		NodeId:   3,
		Children: []int32{2},
	}

	builder.qry.Nodes = []*planpb.Node{scanNode, sortProjectNode, sortNode, projNode}
	return builder, 3
}

func makeTestRegularIndexMessageBuilder(
	t *testing.T,
	prefixArgCount int,
	sortColPos int32,
	sortFlag planpb.OrderBySpec_OrderByFlag,
) (*QueryBuilder, int32) {
	t.Helper()

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)

	scanNode := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		NodeId:   0,
		TableDef: &planpb.TableDef{
			Cols: []*planpb.ColDef{
				{
					Name: catalog.IndexTableIndexColName,
					Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
				},
				{
					Name: catalog.IndexTablePrimaryColName,
					Typ:  planpb.Type{Id: int32(types.T_int64)},
				},
			},
		},
		BindingTags: []int32{100},
		FilterList:  []*planpb.Expr{makeTestRegularIndexPrefixEq(t, prefixArgCount)},
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan:    true,
			IndexName:      "idx_user_active",
			BelongToTable:  "events",
			Parts:          []string{"user_id", "is_active", "__mo_alias_id"},
			IsUnique:       false,
			IndexTableName: "__mo_index_secondary_idx_user_active",
		},
	}

	sortExpr := GetColExpr(scanNode.TableDef.Cols[sortColPos].Typ, 100, sortColPos)
	sortExpr.GetCol().Name = scanNode.TableDef.Cols[sortColPos].Name

	sortNode := &planpb.Node{
		NodeType: planpb.Node_SORT,
		NodeId:   1,
		Children: []int32{0},
		OrderBy: []*planpb.OrderBySpec{
			{
				Expr: sortExpr,
				Flag: sortFlag,
			},
		},
		Limit: &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{
					Value: &planpb.Literal_U64Val{U64Val: 20},
				},
			},
		},
	}

	projNode := &planpb.Node{
		NodeType: planpb.Node_PROJECT,
		NodeId:   2,
		Children: []int32{1},
	}

	builder.qry.Nodes = []*planpb.Node{scanNode, sortNode, projNode}
	return builder, 2
}

func TestApplyIndicesForProjectPushesTopValueThroughRegularIndexPKOrder(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
		planpb.OrderBySpec_DESC,
	)

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortProjectNode := builder.qry.Nodes[1]
	sortNode := builder.qry.Nodes[2]

	require.Len(t, sortNode.SendMsgList, 1)
	assert.Equal(t, int32(message.MsgTopValue), sortNode.SendMsgList[0].MsgType)
	require.Len(t, scanNode.RecvMsgList, 1)
	assert.Equal(t, sortNode.SendMsgList[0], scanNode.RecvMsgList[0])

	require.Len(t, scanNode.OrderBy, 1)
	scanOrderCol := scanNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, scanOrderCol)
	assert.Equal(t, int32(100), scanOrderCol.RelPos)
	assert.Equal(t, int32(0), scanOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, scanOrderCol.Name)
	assert.Equal(t, planpb.OrderBySpec_DESC, scanNode.OrderBy[0].Flag)
	require.NotNil(t, scanNode.IndexReaderParam)
	require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
	assert.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())
	indexParamCol := scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol()
	require.NotNil(t, indexParamCol)
	assert.Equal(t, int32(100), indexParamCol.RelPos)
	assert.Equal(t, int32(0), indexParamCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, indexParamCol.Name)
	assert.Equal(t, planpb.OrderBySpec_DESC, scanNode.IndexReaderParam.OrderBy[0].Flag)

	sortOrderCol := sortNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, sortOrderCol)
	assert.Equal(t, int32(200), sortOrderCol.RelPos)
	assert.Equal(t, int32(1), sortOrderCol.ColPos)

	require.Len(t, sortProjectNode.ProjectList, 2)
	hiddenKeyProjectCol := sortProjectNode.ProjectList[1].GetCol()
	require.NotNil(t, hiddenKeyProjectCol)
	assert.Equal(t, int32(100), hiddenKeyProjectCol.RelPos)
	assert.Equal(t, int32(0), hiddenKeyProjectCol.ColPos)
	assert.Equal(t, "id", builder.nameByColRef[[2]int32{200, 1}])
}

func TestApplyIndicesForProjectSQLCalcFoundRowsSkipsOrderedLimit(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
		planpb.OrderBySpec_DESC,
	)
	builder.sqlCalcFoundRows = true

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[2]
	require.Len(t, scanNode.OrderBy, 1)
	require.Len(t, sortNode.SendMsgList, 1)
	require.Nil(t, scanNode.IndexReaderParam)
}

func TestHandleMessageFromTopToScanSQLCalcFoundRowsSkipsOrderedLimit(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
	builder.sqlCalcFoundRows = true

	builder.handleMessageFromTopToScan(rootNodeID)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[1]
	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	require.Nil(t, scanNode.IndexReaderParam)
}

func TestApplyIndicesForProjectPushesTopValueThroughRegularIndexPKOrderAsc(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
		0,
	)

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[2]

	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	assert.Equal(t, planpb.OrderBySpec_OrderByFlag(0), sortNode.OrderBy[0].Flag)
	assert.Equal(t, planpb.OrderBySpec_OrderByFlag(0), scanNode.OrderBy[0].Flag)
	assert.Equal(t, catalog.IndexTableIndexColName, scanNode.OrderBy[0].Expr.GetCol().Name)
	require.NotNil(t, scanNode.IndexReaderParam)
	require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
	assert.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())
	assert.Equal(t, planpb.OrderBySpec_OrderByFlag(0), scanNode.IndexReaderParam.OrderBy[0].Flag)
	assert.Equal(t, catalog.IndexTableIndexColName, scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol().Name)
}

func TestApplyIndicesForProjectSkipsOrderedLimitForFloatSortKey(t *testing.T) {
	floatType := planpb.Type{Id: int32(types.T_float64)}
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(floatType, 100, 1),
		planpb.OrderBySpec_DESC,
	)
	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[2]
	scanNode.TableDef.Cols[1].Typ = floatType
	sortNode.OrderBy[0].Expr.Typ = floatType

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	// The serialized hidden key is not SQL-order-compatible for floats. Keep
	// the logical float Sort intact and use the index only as an access path.
	require.Empty(t, scanNode.OrderBy)
	require.Empty(t, scanNode.RecvMsgList)
	require.Empty(t, sortNode.SendMsgList)
	require.Equal(t, int32(types.T_float64), sortNode.OrderBy[0].Expr.Typ.Id)
	require.Equal(t, int32(0), sortNode.OrderBy[0].Expr.GetCol().ColPos)
	assert.Nil(t, scanNode.IndexReaderParam)
}

func TestApplyIndicesForProjectSkipsFloatCursorRangeRewrite(t *testing.T) {
	floatType := planpb.Type{Id: int32(types.T_float64)}
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(floatType, 100, 1),
		planpb.OrderBySpec_DESC,
	)
	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[2]
	scanNode.TableDef.Cols[1].Typ = floatType
	sortNode.OrderBy[0].Expr.Typ = floatType
	floatCursor, err := BindFuncExprImplByPlanExpr(context.Background(), "<", []*planpb.Expr{
		GetColExpr(floatType, 100, 1),
		MakePlan2Float64ConstExprWithType(4900),
	})
	require.NoError(t, err)
	scanNode.FilterList = []*planpb.Expr{makeTestRegularIndexPrefixEq(t, 2), floatCursor}

	_, err = builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	assert.Nil(t, scanNode.IndexReaderParam)
	require.Empty(t, scanNode.OrderBy)
	require.Empty(t, sortNode.SendMsgList)
	assert.True(t, isRegularIndexFullPrefixEquality(scanNode.FilterList[0], 2))
}

func TestApplyIndicesForProjectSkipsOrderedLimitWithAdditionalResidualFilter(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
		planpb.OrderBySpec_DESC,
	)
	scanNode := builder.qry.Nodes[0]
	scanNode.FilterList = append(scanNode.FilterList, makeTestRegularIndexPKLessThan(t, 4900))
	scanNode.FilterList = append(scanNode.FilterList, makeTestRegularIndexPKRange(t, ">", 100, 1, 100, false))

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[2]
	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	assert.Equal(t, catalog.IndexTableIndexColName, scanNode.OrderBy[0].Expr.GetCol().Name)
	assert.Nil(t, scanNode.IndexReaderParam)
}

func TestApplyIndicesForProjectPushesOrderedLimitWithCursorRange(t *testing.T) {
	for _, op := range []string{"<", "<=", ">", ">="} {
		t.Run(op, func(t *testing.T) {
			builder, rootNodeID := makeTestRegularIndexProjectBuilder(
				t,
				2,
				GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
				planpb.OrderBySpec_DESC,
			)
			scanNode := builder.qry.Nodes[0]
			scanNode.FilterList = []*planpb.Expr{
				makeTestRegularIndexPrefixEq(t, 2),
				makeTestRegularIndexPKRange(t, op, 100, 1, 4900, false),
			}

			_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)

			require.NotNil(t, scanNode.IndexReaderParam)
			requireTestRegularIndexCursorRange(t, scanNode.FilterList[0], 2, op)
			require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
			assert.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())
			assert.Equal(t, catalog.IndexTableIndexColName, scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol().Name)
		})
	}
}

func TestHandleMessageFromTopToScanRewritesRegularIndexPKOrderToHiddenKey(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)

	builder.handleMessageFromTopToScan(rootNodeID)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[1]

	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	require.NotNil(t, scanNode.IndexReaderParam)
	require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
	assert.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())

	sortOrderCol := sortNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, sortOrderCol)
	assert.Equal(t, int32(100), sortOrderCol.RelPos)
	assert.Equal(t, int32(0), sortOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTablePrimaryColName, sortOrderCol.Name)

	scanOrderCol := scanNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, scanOrderCol)
	assert.Equal(t, int32(100), scanOrderCol.RelPos)
	assert.Equal(t, int32(0), scanOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, scanOrderCol.Name)
	indexParamCol := scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol()
	require.NotNil(t, indexParamCol)
	assert.Equal(t, int32(100), indexParamCol.RelPos)
	assert.Equal(t, int32(0), indexParamCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, indexParamCol.Name)
}

func TestHandleMessageFromTopToScanSkipsOrderedLimitForFloatSortKey(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
	floatType := planpb.Type{Id: int32(types.T_float64)}
	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[1]
	scanNode.TableDef.Cols[1].Typ = floatType
	sortNode.OrderBy[0].Expr.Typ = floatType

	builder.handleMessageFromTopToScan(rootNodeID)

	require.Empty(t, scanNode.OrderBy)
	require.Empty(t, scanNode.RecvMsgList)
	require.Empty(t, sortNode.SendMsgList)
	require.Equal(t, int32(types.T_float64), sortNode.OrderBy[0].Expr.Typ.Id)
	require.Equal(t, int32(1), sortNode.OrderBy[0].Expr.GetCol().ColPos)
	assert.Nil(t, scanNode.IndexReaderParam)
}

func TestHandleMessageFromTopToScanThroughDirectProjection(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	scanTag := builder.genNewBindTag()
	projectTag := builder.genNewBindTag()
	colType := planpb.Type{Id: int32(types.T_int64)}

	scanID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{scanTag},
		TableDef: &planpb.TableDef{
			Name:          "t",
			Cols:          []*planpb.ColDef{{Name: "id", Typ: colType}},
			Name2ColIndex: map[string]int32{"id": 0},
		},
	}, ctx)
	projectID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{scanID},
		BindingTags: []int32{projectTag},
		ProjectList: []*planpb.Expr{GetColExpr(colType, scanTag, 0)},
	}, ctx)
	sortID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_SORT,
		Children: []int32{projectID},
		OrderBy: []*planpb.OrderBySpec{{
			Expr: GetColExpr(colType, projectTag, 0),
		}},
		Limit: makePlan2Uint64ConstExprWithType(10),
	}, ctx)

	builder.handleMessageFromTopToScan(sortID)

	scan := builder.qry.Nodes[scanID]
	require.Len(t, builder.qry.Nodes[sortID].SendMsgList, 1)
	require.Len(t, scan.RecvMsgList, 1)
	require.Len(t, scan.OrderBy, 1)
	require.Equal(t, scanTag, scan.OrderBy[0].Expr.GetCol().RelPos)
	require.Equal(t, int32(0), scan.OrderBy[0].Expr.GetCol().ColPos)
}

func TestHandleMessageFromTopToScanPushesRegularIndexLimitThroughDirectProjection(t *testing.T) {
	builder, rootID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
	sortID := builder.qry.Nodes[rootID].Children[0]
	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[sortID]
	projectTag := int32(200)
	projectID := int32(len(builder.qry.Nodes))
	builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		NodeId:      projectID,
		Children:    []int32{scanNode.NodeId},
		BindingTags: []int32{projectTag},
		ProjectList: []*planpb.Expr{GetColExpr(scanNode.TableDef.Cols[1].Typ, scanNode.BindingTags[0], 1)},
	})
	sortNode.Children[0] = projectID
	sortNode.OrderBy[0].Expr = GetColExpr(scanNode.TableDef.Cols[1].Typ, projectTag, 0)

	builder.handleMessageFromTopToScan(sortID)

	require.NotNil(t, scanNode.IndexReaderParam)
	require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
	require.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())
	require.Equal(t, scanNode.BindingTags[0], scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol().RelPos)
	require.Equal(t, catalog.IndexTableIndexColName, scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol().Name)
}

func TestHandleMessageFromTopToScanSkipsSortWithoutOrderKey(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.qry.Nodes = []*planpb.Node{
		{NodeType: planpb.Node_TABLE_SCAN, NodeId: 0},
		{NodeType: planpb.Node_SORT, NodeId: 1, Children: []int32{0}, Limit: makePlan2Uint64ConstExprWithType(1)},
	}

	require.NotPanics(t, func() { builder.handleMessageFromTopToScan(1) })
	require.Empty(t, builder.qry.Nodes[0].RecvMsgList)
}

func TestHandleMessageFromTopToScanPreservesShuffleOnRejectedPath(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()
	left := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, NodeId: 0, BindingTags: []int32{leftTag}}
	right := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, NodeId: 1, BindingTags: []int32{rightTag}}
	join := &planpb.Node{
		NodeType:               planpb.Node_JOIN,
		NodeId:                 2,
		JoinType:               planpb.Node_INNER,
		Children:               []int32{0, 1},
		Stats:                  &planpb.Stats{HashmapStats: &planpb.HashMapStats{Shuffle: true}},
		RuntimeFilterProbeList: []*planpb.RuntimeFilterSpec{{Tag: 1}},
		RuntimeFilterBuildList: []*planpb.RuntimeFilterSpec{{Tag: 1}},
	}
	sort := &planpb.Node{
		NodeType: planpb.Node_SORT,
		NodeId:   3,
		Children: []int32{2},
		OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rightTag, ColPos: 0}},
		}}},
		Limit: makePlan2Uint64ConstExprWithType(1),
	}
	builder.qry.Nodes = []*planpb.Node{left, right, join, sort}

	builder.handleMessageFromTopToScan(3)

	require.True(t, join.Stats.HashmapStats.Shuffle)
	require.Len(t, join.RuntimeFilterProbeList, 1)
	require.Len(t, join.RuntimeFilterBuildList, 1)
	require.Empty(t, sort.SendMsgList)
}

func TestHandleMessageFromTopToScanDisablesShuffleOnAcceptedPath(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()
	left := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, NodeId: 0, BindingTags: []int32{leftTag}}
	right := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, NodeId: 1, BindingTags: []int32{rightTag}}
	join := &planpb.Node{
		NodeType:               planpb.Node_JOIN,
		NodeId:                 2,
		JoinType:               planpb.Node_INNER,
		Children:               []int32{0, 1},
		Stats:                  &planpb.Stats{HashmapStats: &planpb.HashMapStats{Shuffle: true}},
		RuntimeFilterProbeList: []*planpb.RuntimeFilterSpec{{Tag: 1}},
		RuntimeFilterBuildList: []*planpb.RuntimeFilterSpec{{Tag: 1}},
	}
	sort := &planpb.Node{
		NodeType: planpb.Node_SORT,
		NodeId:   3,
		Children: []int32{2},
		OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: leftTag, ColPos: 0}},
		}}},
		Limit: makePlan2Uint64ConstExprWithType(1),
	}
	builder.qry.Nodes = []*planpb.Node{left, right, join, sort}

	builder.handleMessageFromTopToScan(3)

	require.False(t, join.Stats.HashmapStats.Shuffle)
	require.Empty(t, join.RuntimeFilterProbeList)
	require.Empty(t, join.RuntimeFilterBuildList)
	require.Len(t, sort.SendMsgList, 1)
	require.Len(t, left.RecvMsgList, 1)
}

func TestHandleMessageFromTopToScanSkipsOrderedLimitWithAdditionalResidualFilter(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
	scanNode := builder.qry.Nodes[0]
	scanNode.FilterList = append(scanNode.FilterList, makeTestRegularIndexPKLessThan(t, 4900))
	scanNode.FilterList = append(scanNode.FilterList, makeTestRegularIndexPKRange(t, ">", 100, 1, 100, false))

	builder.handleMessageFromTopToScan(rootNodeID)

	sortNode := builder.qry.Nodes[1]
	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	assert.Equal(t, catalog.IndexTableIndexColName, scanNode.OrderBy[0].Expr.GetCol().Name)
	assert.Nil(t, scanNode.IndexReaderParam)
}

func TestHandleMessageFromTopToScanPushesOrderedLimitWithCursorRange(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
	scanNode := builder.qry.Nodes[0]
	scanNode.FilterList = []*planpb.Expr{
		makeTestRegularIndexPrefixEq(t, 2),
		makeTestRegularIndexPKLessThan(t, 4900),
	}

	builder.handleMessageFromTopToScan(rootNodeID)

	require.NotNil(t, scanNode.IndexReaderParam)
	requireTestRegularIndexCursorRange(t, scanNode.FilterList[0], 2, "<")
	require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
	assert.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())
}

func TestHandleMessageFromTopToScanSkipsOrderedLimitAcrossFilter(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
	scanNode := builder.qry.Nodes[0]
	scanNode.FilterList = []*planpb.Expr{
		makeTestRegularIndexPrefixEq(t, 2),
		makeTestRegularIndexPKLessThan(t, 4900),
	}

	filterID := int32(len(builder.qry.Nodes))
	builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
		NodeType:   planpb.Node_FILTER,
		NodeId:     filterID,
		Children:   []int32{0},
		FilterList: []*planpb.Expr{makeTestRegularIndexPKLessThan(t, 4800)},
	})
	builder.qry.Nodes[1].Children[0] = filterID

	builder.handleMessageFromTopToScan(rootNodeID)

	sortNode := builder.qry.Nodes[1]
	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	assert.Nil(t, scanNode.IndexReaderParam)
	assert.True(t, isRegularIndexFullPrefixEquality(scanNode.FilterList[0], 2))
}

func TestHandleMessageFromTopToScanSkipsOrderedLimitAcrossCardinalityReducingJoin(t *testing.T) {
	for _, joinType := range []planpb.Node_JoinType{planpb.Node_INNER, planpb.Node_SEMI} {
		t.Run(joinType.String(), func(t *testing.T) {
			builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
			scanNode := builder.qry.Nodes[0]
			scanNode.FilterList = []*planpb.Expr{
				makeTestRegularIndexPrefixEq(t, 2),
				makeTestRegularIndexPKLessThan(t, 4900),
			}

			rightScanID := int32(len(builder.qry.Nodes))
			builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
				NodeType:    planpb.Node_TABLE_SCAN,
				NodeId:      rightScanID,
				BindingTags: []int32{300},
				TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
					{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
				}},
			})
			joinCond, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{
				GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
				GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 300, 0),
			})
			require.NoError(t, err)

			joinID := int32(len(builder.qry.Nodes))
			builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
				NodeType: planpb.Node_JOIN,
				NodeId:   joinID,
				Children: []int32{0, rightScanID},
				JoinType: joinType,
				OnList:   []*planpb.Expr{joinCond},
				Stats: &planpb.Stats{
					HashmapStats: &planpb.HashMapStats{},
				},
			})
			builder.qry.Nodes[1].Children[0] = joinID

			builder.handleMessageFromTopToScan(rootNodeID)

			sortNode := builder.qry.Nodes[1]
			require.Len(t, sortNode.SendMsgList, 1)
			require.Len(t, scanNode.RecvMsgList, 1)
			require.Len(t, scanNode.OrderBy, 1)
			assert.Nil(t, scanNode.IndexReaderParam)
			assert.True(t, isRegularIndexFullPrefixEquality(scanNode.FilterList[0], 2))
		})
	}
}

func TestHandleMessageFromTopToScanKeepsPKOrderWhenPrefixIncomplete(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 1, 1, planpb.OrderBySpec_DESC)

	builder.handleMessageFromTopToScan(rootNodeID)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[1]

	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	assert.Nil(t, scanNode.IndexReaderParam)

	sortOrderCol := sortNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, sortOrderCol)
	assert.Equal(t, int32(1), sortOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTablePrimaryColName, sortOrderCol.Name)

	scanOrderCol := scanNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, scanOrderCol)
	assert.Equal(t, int32(1), scanOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTablePrimaryColName, scanOrderCol.Name)
}

func TestRegularIndexFullPrefixEqualityRequiresComparisonSerial(t *testing.T) {
	assert.True(t, isRegularIndexFullPrefixEquality(makeTestRegularIndexPrefixEq(t, 2), 2))
	assert.False(t, isRegularIndexFullPrefixEquality(makeTestRegularIndexPrefixEqWithSerialFunc(t, 2, "serial_full"), 2))
}

func TestRewriteRegularIndexCursorRangeFilter(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	scanNode := &planpb.Node{
		BindingTags: []int32{100},
		TableDef: &planpb.TableDef{
			Cols: []*planpb.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
				{Name: catalog.IndexTablePrimaryColName, Typ: planpb.Type{Id: int32(types.T_int64)}},
			},
		},
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan: true,
			Parts:       []string{"user_id", "is_active", "__mo_alias_id"},
		},
	}

	for _, tc := range []struct {
		name       string
		op         string
		constFirst bool
		expectedOp string
	}{
		{name: "less", op: "<", expectedOp: "<"},
		{name: "less reversed", op: ">", constFirst: true, expectedOp: "<"},
		{name: "less equal", op: "<=", expectedOp: "<="},
		{name: "greater", op: ">", expectedOp: ">"},
		{name: "greater reversed", op: "<", constFirst: true, expectedOp: ">"},
		{name: "greater equal", op: ">=", expectedOp: ">="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursorFilter := makeTestRegularIndexPKRange(t, tc.op, 100, 1, 4900, tc.constFirst)
			scanNode.FilterList = []*planpb.Expr{
				makeTestRegularIndexPrefixEq(t, 2),
				cursorFilter,
			}
			require.True(t, builder.rewriteRegularIndexCursorRangeFilter(scanNode))
			cursor := requireTestRegularIndexCursorRange(t, scanNode.FilterList[0], 2, tc.expectedOp)
			assert.Equal(t, int64(4900), cursor.GetLit().GetI64Val())
			assert.Same(t, cursorFilter, scanNode.FilterList[1])
		})
	}

	scanNode.FilterList = []*planpb.Expr{
		makeTestRegularIndexPrefixEq(t, 2),
		makeTestRegularIndexPKRange(t, "<", 100, 1, 4900, false),
		makeTestRegularIndexPKRange(t, ">", 100, 1, 100, false),
	}
	assert.False(t, builder.rewriteRegularIndexCursorRangeFilter(scanNode))
	assert.True(t, isRegularIndexFullPrefixEquality(scanNode.FilterList[0], 2))

	paramCursor, err := BindFuncExprImplByPlanExpr(context.Background(), "<", []*planpb.Expr{
		GetColExpr(scanNode.TableDef.Cols[1].Typ, 100, 1),
		{
			Typ:  scanNode.TableDef.Cols[1].Typ,
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
		},
	})
	require.NoError(t, err)
	scanNode.FilterList = []*planpb.Expr{makeTestRegularIndexPrefixEq(t, 2), paramCursor}
	assert.True(t, builder.rewriteRegularIndexCursorRangeFilter(scanNode))

	computedCursorValue, err := BindFuncExprImplByPlanExpr(context.Background(), "+", []*planpb.Expr{
		MakePlan2Int64ConstExprWithType(4899),
		MakePlan2Int64ConstExprWithType(1),
	})
	require.NoError(t, err)
	computedCursor, err := BindFuncExprImplByPlanExpr(context.Background(), "<", []*planpb.Expr{
		GetColExpr(scanNode.TableDef.Cols[1].Typ, 100, 1),
		computedCursorValue,
	})
	require.NoError(t, err)
	scanNode.FilterList = []*planpb.Expr{makeTestRegularIndexPrefixEq(t, 2), computedCursor}
	assert.False(t, builder.rewriteRegularIndexCursorRangeFilter(scanNode))
	assert.True(t, isRegularIndexFullPrefixEquality(scanNode.FilterList[0], 2))

	scanNode.TableDef.Cols[1].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 64}
	stringCursor, err := BindFuncExprImplByPlanExpr(context.Background(), "<", []*planpb.Expr{
		GetColExpr(scanNode.TableDef.Cols[1].Typ, 100, 1),
		{
			Typ: planpb.Type{Id: int32(types.T_varchar), Width: 11},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "id_00004900"}},
			},
		},
	})
	require.NoError(t, err)
	scanNode.FilterList = []*planpb.Expr{makeTestRegularIndexPrefixEq(t, 2), stringCursor}
	assert.True(t, builder.rewriteRegularIndexCursorRangeFilter(scanNode))
	requireTestRegularIndexCursorRange(t, scanNode.FilterList[0], 2, "<")
}

func TestApplyIndicesForProjectSkipsRegularIndexPKOrderWithoutFullPrefixEquality(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		1,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
		planpb.OrderBySpec_DESC,
	)

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortProjectNode := builder.qry.Nodes[1]
	sortNode := builder.qry.Nodes[2]

	assert.Empty(t, sortNode.SendMsgList)
	assert.Empty(t, scanNode.RecvMsgList)
	assert.Empty(t, scanNode.OrderBy)
	assert.Nil(t, scanNode.IndexReaderParam)
	require.Len(t, sortProjectNode.ProjectList, 1)
}

func TestApplyIndicesForProjectSkipsRegularIndexPKOrderForNonPKSortColumn(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, 100, 0),
		planpb.OrderBySpec_DESC,
	)

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortProjectNode := builder.qry.Nodes[1]
	sortNode := builder.qry.Nodes[2]

	assert.Empty(t, sortNode.SendMsgList)
	assert.Empty(t, scanNode.RecvMsgList)
	assert.Empty(t, scanNode.OrderBy)
	assert.Nil(t, scanNode.IndexReaderParam)
	require.Len(t, sortProjectNode.ProjectList, 1)
}

func TestHandleMessageFromTopToScanSkipsOrderedLimitForOffsetOrRank(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*planpb.Node)
	}{
		{
			name: "offset",
			setup: func(sortNode *planpb.Node) {
				sortNode.Offset = &planpb.Expr{
					Typ: planpb.Type{Id: int32(types.T_uint64)},
					Expr: &planpb.Expr_Lit{
						Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 3}},
					},
				}
			},
		},
		{
			name: "rank",
			setup: func(sortNode *planpb.Node) {
				sortNode.RankOption = &planpb.RankOption{}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
			sortNode := builder.qry.Nodes[1]
			scanNode := builder.qry.Nodes[0]
			scanNode.FilterList = []*planpb.Expr{
				makeTestRegularIndexPrefixEq(t, 2),
				makeTestRegularIndexPKLessThan(t, 4900),
			}
			tc.setup(sortNode)

			builder.handleMessageFromTopToScan(rootNodeID)

			require.Len(t, sortNode.SendMsgList, 1)
			require.Len(t, scanNode.RecvMsgList, 1)
			require.Len(t, scanNode.OrderBy, 1)
			assert.Nil(t, scanNode.IndexReaderParam)
			assert.True(t, isRegularIndexFullPrefixEquality(scanNode.FilterList[0], 2))
		})
	}
}

func TestApplyIndicesForProjectSkipsOrderedLimitForOffsetOrRank(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*planpb.Node)
	}{
		{
			name: "offset",
			setup: func(sortNode *planpb.Node) {
				sortNode.Offset = &planpb.Expr{
					Typ: planpb.Type{Id: int32(types.T_uint64)},
					Expr: &planpb.Expr_Lit{
						Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 3}},
					},
				}
			},
		},
		{
			name: "rank",
			setup: func(sortNode *planpb.Node) {
				sortNode.RankOption = &planpb.RankOption{}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, rootNodeID := makeTestRegularIndexProjectBuilder(
				t,
				2,
				GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
				planpb.OrderBySpec_DESC,
			)
			sortNode := builder.qry.Nodes[2]
			scanNode := builder.qry.Nodes[0]
			scanNode.FilterList = []*planpb.Expr{
				makeTestRegularIndexPrefixEq(t, 2),
				makeTestRegularIndexPKLessThan(t, 4900),
			}
			tc.setup(sortNode)

			_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)

			assert.Empty(t, sortNode.SendMsgList)
			assert.Empty(t, scanNode.RecvMsgList)
			assert.Empty(t, scanNode.OrderBy)
			assert.Nil(t, scanNode.IndexReaderParam)
			assert.True(t, isRegularIndexFullPrefixEquality(scanNode.FilterList[0], 2))
		})
	}
}
func TestTryMatchMoreLeadingFiltersRequiresContiguousPrefix(t *testing.T) {
	idxDef := &IndexDef{
		Parts: []string{"uid", "typ", "flag", "__mo_alias_id"},
	}
	node := &planpb.Node{
		TableDef: &planpb.TableDef{
			Name2ColIndex: map[string]int32{
				"uid":  1,
				"typ":  2,
				"flag": 3,
				"id":   0,
			},
		},
		// Filters only on uid and flag, missing typ.
		FilterList: []*planpb.Expr{
			makeEqFilterExpr(1),
			makeEqFilterExpr(3),
		},
	}

	leadingPos := tryMatchMoreLeadingFilters(idxDef, node, 0)
	if !reflect.DeepEqual([]int32{0}, leadingPos) {
		t.Fatalf("unexpected leading positions, got=%v, want=%v", leadingPos, []int32{0})
	}
}

func TestGetIndexForNonEquiCond_DetectsPairedRangeOnIndexColumn(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	idxDef := &IndexDef{
		IndexName:      "idx_price",
		Parts:          []string{"price", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "__mo_index_secondary_idx_price",
	}

	node := &planpb.Node{
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name2ColIndex: map[string]int32{
				catalog.FakePrimaryKeyColName: 0,
				"price":                       1,
			},
			Cols: []*planpb.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
				{Name: "price", Typ: planpb.Type{Id: int32(types.T_int64)}},
			},
			Pkey:    &planpb.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			Indexes: []*planpb.IndexDef{idxDef},
		},
		FilterList: []*planpb.Expr{
			makeRangeFilterExpr(bindTag, 1, ">=", 99),
			makeRangeFilterExpr(bindTag, 1, "<=", 299),
		},
	}

	idxPos, filterIdx := builder.getIndexForNonEquiCond([]*planpb.IndexDef{idxDef}, node)
	require.Equal(t, 0, idxPos)
	require.ElementsMatch(t, []int32{0, 1}, filterIdx)
}

func TestGetIndexForNonEquiCondSkipsNonEqualityOnDeclaredPrefix(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	prefixParams := `{"prefix_lengths":"name:3"}`
	makeIndex := func(unique bool, parts []string, params string) *planpb.IndexDef {
		return &planpb.IndexDef{
			IndexName:       "idx_name",
			Parts:           parts,
			Unique:          unique,
			IndexTableName:  "__mo_index_secondary_idx_name",
			IndexAlgoParams: params,
		}
	}
	makeOr := func(args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "or"},
				Args: args,
			}},
		}
	}
	makeInRange := func() *planpb.Expr {
		expr := makeStringBetweenFilterExpr(bindTag, 1, "abcX", "abdA")
		expr.GetF().Func.ObjName = "in_range"
		return expr
	}

	tests := []struct {
		name        string
		idxDef      *planpb.IndexDef
		filters     []*planpb.Expr
		wantIdx     int
		wantFilters []int32
	}{
		{
			name:    "non-unique prefix between",
			idxDef:  makeIndex(false, []string{"name", catalog.FakePrimaryKeyColName}, prefixParams),
			filters: []*planpb.Expr{makeStringBetweenFilterExpr(bindTag, 1, "abcX", "abdA")},
			wantIdx: -1,
		},
		{
			name:    "unique prefix between",
			idxDef:  makeIndex(true, []string{"name"}, prefixParams),
			filters: []*planpb.Expr{makeStringBetweenFilterExpr(bindTag, 1, "abcX", "abdA")},
			wantIdx: -1,
		},
		{
			name:    "prefix single lower bound",
			idxDef:  makeIndex(false, []string{"name", catalog.FakePrimaryKeyColName}, prefixParams),
			filters: []*planpb.Expr{makeRangeFilterExpr(bindTag, 1, ">=", 99)},
			wantIdx: -1,
		},
		{
			name:   "prefix paired range",
			idxDef: makeIndex(false, []string{"name", catalog.FakePrimaryKeyColName}, prefixParams),
			filters: []*planpb.Expr{
				makeRangeFilterExpr(bindTag, 1, ">=", 99),
				makeRangeFilterExpr(bindTag, 1, "<=", 299),
			},
			wantIdx: -1,
		},
		{
			name:    "prefix in range",
			idxDef:  makeIndex(false, []string{"name", catalog.FakePrimaryKeyColName}, prefixParams),
			filters: []*planpb.Expr{makeInRange()},
			wantIdx: -1,
		},
		{
			name:   "prefix or containing range",
			idxDef: makeIndex(false, []string{"name", catalog.FakePrimaryKeyColName}, prefixParams),
			filters: []*planpb.Expr{makeOr(
				makeStringBetweenFilterExpr(bindTag, 1, "abcX", "abdA"),
				makeStringInFilterExpr(bindTag, 1, "abe"),
			)},
			wantIdx: -1,
		},
		{
			name:    "prefix in is rejected with other non equality predicates",
			idxDef:  makeIndex(false, []string{"name", catalog.FakePrimaryKeyColName}, prefixParams),
			filters: []*planpb.Expr{makeStringInFilterExpr(bindTag, 1, "abcX", "abdA")},
			wantIdx: -1,
		},
		{
			name:        "complete index between remains eligible",
			idxDef:      makeIndex(false, []string{"name", catalog.FakePrimaryKeyColName}, ""),
			filters:     []*planpb.Expr{makeStringBetweenFilterExpr(bindTag, 1, "abcX", "abdA")},
			wantIdx:     0,
			wantFilters: []int32{0},
		},
		{
			name:    "non-leading declared prefix also rejects range",
			idxDef:  makeIndex(false, []string{"name", "suffix", catalog.FakePrimaryKeyColName}, `{"prefix_lengths":"suffix:3"}`),
			filters: []*planpb.Expr{makeStringBetweenFilterExpr(bindTag, 1, "abcX", "abdA")},
			wantIdx: -1,
		},
		{
			name:    "invalid prefix metadata fails closed for range",
			idxDef:  makeIndex(false, []string{"name", catalog.FakePrimaryKeyColName}, "{bad json"),
			filters: []*planpb.Expr{makeStringBetweenFilterExpr(bindTag, 1, "abcX", "abdA")},
			wantIdx: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &planpb.Node{
				BindingTags: []int32{bindTag},
				TableDef: &planpb.TableDef{
					Name2ColIndex: map[string]int32{
						catalog.FakePrimaryKeyColName: 0,
						"name":                        1,
						"suffix":                      2,
					},
					Cols: []*planpb.ColDef{
						{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
						{Name: "name", Typ: planpb.Type{Id: int32(types.T_varchar)}},
						{Name: "suffix", Typ: planpb.Type{Id: int32(types.T_varchar)}},
					},
					Pkey:    &planpb.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
					Indexes: []*planpb.IndexDef{tt.idxDef},
				},
				FilterList: tt.filters,
			}

			idxPos, filterIdx := builder.getIndexForNonEquiCond([]*planpb.IndexDef{tt.idxDef}, node)
			require.Equal(t, tt.wantIdx, idxPos)
			require.ElementsMatch(t, tt.wantFilters, filterIdx)
		})
	}
}

func TestGetIndexForNonEquiCondUsesCompleteRangeIndexAlternative(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	prefixIdx := &planpb.IndexDef{
		IndexName:       "idx_name_prefix",
		Parts:           []string{"name", catalog.FakePrimaryKeyColName},
		IndexTableName:  "__mo_index_secondary_idx_name_prefix",
		IndexAlgoParams: `{"prefix_lengths":"name:3"}`,
	}
	completeIdx := &planpb.IndexDef{
		IndexName:      "idx_name_complete",
		Parts:          []string{"name", catalog.FakePrimaryKeyColName},
		IndexTableName: "__mo_index_secondary_idx_name_complete",
	}

	for _, tt := range []struct {
		name    string
		indexes []*planpb.IndexDef
		wantIdx int
	}{
		{name: "prefix then complete", indexes: []*planpb.IndexDef{prefixIdx, completeIdx}, wantIdx: 1},
		{name: "complete then prefix", indexes: []*planpb.IndexDef{completeIdx, prefixIdx}, wantIdx: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node := &planpb.Node{
				TableDef: &planpb.TableDef{
					Name2ColIndex: map[string]int32{
						catalog.FakePrimaryKeyColName: 0,
						"name":                        1,
					},
					Cols: []*planpb.ColDef{
						{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
						{Name: "name", Typ: planpb.Type{Id: int32(types.T_varchar)}},
					},
					Indexes: tt.indexes,
				},
				FilterList: []*planpb.Expr{
					makeStringBetweenFilterExpr(bindTag, 1, "abcX", "abdA"),
				},
			}

			idxPos, filterIdx := builder.getIndexForNonEquiCond(tt.indexes, node)
			require.Equal(t, tt.wantIdx, idxPos)
			require.Equal(t, []int32{0}, filterIdx)
		})
	}
}

func TestScopedForceHintsRejectLossyPrefixIndex(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "order by",
			sql:  "select id from index_hint_t force index for order by(idx_a) where a between 'abcX' and 'abdA' order by a",
		},
		{
			name: "group by",
			sql:  "select a, count(*) from index_hint_t force index for group by(idx_a) where a between 'abcX' and 'abdA' group by a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			tableDef := mock.ctxt.tables["index_hint_t"]
			tableDef.Cols[1].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 32}
			tableDef.Indexes[0].IndexAlgoParams = `{"prefix_lengths":"a:3"}`

			queryPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			require.Empty(t, findFirstIndexScanName(queryPlan))
		})
	}
}
func TestGetIndexForNonEquiCond_SkipsLargePairedRangeByStats(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	idxDef := &IndexDef{
		IndexName:      "idx_price",
		Parts:          []string{"price", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "__mo_index_secondary_idx_price",
	}

	node := &planpb.Node{
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name2ColIndex: map[string]int32{
				catalog.FakePrimaryKeyColName: 0,
				"price":                       1,
			},
			Cols: []*planpb.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
				{Name: "price", Typ: planpb.Type{Id: int32(types.T_int64)}},
			},
			Pkey:    &planpb.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			Indexes: []*planpb.IndexDef{idxDef},
		},
		Stats: &planpb.Stats{
			TableCnt:    100000,
			Outcnt:      float64(InFilterCardLimitNonPK),
			Selectivity: 0.05,
		},
		FilterList: []*planpb.Expr{
			makeRangeFilterExpr(bindTag, 1, ">=", 99),
			makeRangeFilterExpr(bindTag, 1, "<=", 299),
		},
	}

	idxPos, filterIdx := builder.getIndexForNonEquiCond([]*planpb.IndexDef{idxDef}, node)
	require.Equal(t, -1, idxPos)
	require.Nil(t, filterIdx)
}

func TestIndexTableSerialFunctionsSeparateStorageFromComparison(t *testing.T) {
	assert.Equal(t, "serial_full", indexTableStoredKeySerialFunc(&planpb.IndexDef{
		Parts:  []string{"status", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}))
	assert.Equal(t, "serial", indexTableStoredKeySerialFunc(&planpb.IndexDef{
		Parts:  []string{"status", "due"},
		Unique: true,
	}))
	assert.Equal(t, "serial", indexTableStoredKeySerialFunc(&planpb.IndexDef{
		Parts:  []string{"status"},
		Unique: false,
	}))
	assert.Equal(t, "serial", indexTableComparisonSerialFunc())
}

func TestRegularIndexPrefixMetadataUsable(t *testing.T) {
	tests := []struct {
		name   string
		parts  []string
		params string
		want   bool
	}{
		{name: "no prefix metadata", parts: []string{"name"}, want: true},
		{name: "matching legacy metadata", parts: []string{"name"}, params: `{"prefix_lengths":"name:4"}`, want: true},
		{name: "matching v2 metadata", parts: []string{"head:line"}, params: `{"prefix_lengths_v2":"{\"head:line\":4}"}`, want: true},
		{name: "non-canonical case fails closed", parts: []string{"name"}, params: `{"prefix_lengths":"Name:4"}`, want: false},
		{name: "stale renamed part", parts: []string{"renamed"}, params: `{"prefix_lengths":"name:4"}`, want: false},
		{name: "malformed metadata", parts: []string{"name"}, params: `{"prefix_lengths":"name:0"}`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, regularIndexPrefixMetadataUsable(&planpb.IndexDef{
				Parts:           tt.parts,
				IndexAlgoParams: tt.params,
			}))
		})
	}

	fullIndex := &planpb.IndexDef{TableExist: true, Parts: []string{"name"}}
	prefixIndex := &planpb.IndexDef{TableExist: true, Parts: []string{"name"}, IndexAlgoParams: `{"prefix_lengths":"name:4"}`}
	staleIndex := &planpb.IndexDef{TableExist: true, Parts: []string{"renamed"}, IndexAlgoParams: `{"prefix_lengths":"name:4"}`}
	require.True(t, usableRegularHintIndex(fullIndex))
	require.False(t, usableRegularHintIndex(prefixIndex))
	require.False(t, usableRegularHintIndex(staleIndex))
	require.NoError(t, validateRegularIndexPrefixMetadata(prefixIndex))
	require.ErrorContains(t, validateRegularIndexPrefixMetadata(staleIndex), "rebuild the index")
	require.ErrorContains(t, validateTableRegularIndexPrefixMetadata(&planpb.TableDef{
		Indexes: []*planpb.IndexDef{staleIndex},
	}), "rebuild the index")
}

func TestGetIndexForNonEquiCondSkipsDeclaredPrefixIndexes(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	node := &planpb.Node{
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name2ColIndex: map[string]int32{"id": 0, "name": 1},
			Cols: []*planpb.ColDef{
				{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "name", Typ: planpb.Type{Id: int32(types.T_varchar)}},
			},
			Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		},
	}
	prefixNonUnique := &planpb.IndexDef{
		IndexName:       "idx_name_prefix",
		Parts:           []string{"name", catalog.CreateAlias("id")},
		IndexAlgoParams: `{"prefix_lengths":"name:4"}`,
	}
	prefixUnique := &planpb.IndexDef{
		IndexName:       "uq_name_prefix",
		Parts:           []string{"name"},
		Unique:          true,
		IndexAlgoParams: `{"prefix_lengths":"name:4"}`,
	}

	filters := []struct {
		name string
		expr *planpb.Expr
	}{
		{name: "in", expr: makeStringInFilterExpr(bindTag, 1, "abcdx", "abcex")},
		{name: "between", expr: makeStringBetweenFilterExpr(bindTag, 1, "abcdx", "abcex")},
		{name: "range", expr: makeRangeFilterExpr(bindTag, 1, ">=", 99)},
	}
	for _, idxDef := range []*planpb.IndexDef{prefixNonUnique, prefixUnique} {
		for _, filter := range filters {
			t.Run(idxDef.IndexName+"/"+filter.name, func(t *testing.T) {
				node.FilterList = []*planpb.Expr{filter.expr}
				idxPos, filterIdx := builder.getIndexForNonEquiCond([]*planpb.IndexDef{idxDef}, node)
				require.Equal(t, -1, idxPos)
				require.Nil(t, filterIdx)
			})
		}
	}

	complete := &planpb.IndexDef{IndexName: "idx_name_full", Parts: []string{"name", catalog.CreateAlias("id")}}
	node.FilterList = []*planpb.Expr{makeStringInFilterExpr(bindTag, 1, "abcdx", "abcex")}
	idxPos, filterIdx := builder.getIndexForNonEquiCond([]*planpb.IndexDef{prefixNonUnique, complete}, node)
	require.Equal(t, 1, idxPos)
	require.Equal(t, []int32{0}, filterIdx)
}

func TestMakeIndexLookupPartExprDoesNotFailOpen(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:           []string{"name"},
		IndexAlgoParams: `{"prefix_lengths":"name:0"}`,
	}

	expr, err := builder.makeIndexLookupPartExpr(idxDef, 0, makePlan2StringConstExprWithType("abcdx"))
	require.Error(t, err)
	require.Nil(t, expr)
}

func TestReplaceEqualConditionUsesNullPropagatingSerialForNonUniqueCompositeIndex(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:  []string{"status", "due", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}
	idxTableDef := makeTestIndexTableDef()
	filters := []*planpb.Expr{makeStringEqFilterExpr(0, 3, "active")}

	expr, err := builder.replaceEqualCondition(idxDef, filters, []int32{0}, 42, idxTableDef)
	require.NoError(t, err)

	require.NotNil(t, expr.GetF())
	require.Equal(t, "prefix_eq", expr.GetF().Func.ObjName)
	assert.Equal(t, "serial", wrappedSerialFuncName(t, expr.GetF().Args[1]))
}

func TestReplaceEqualConditionKeepsSerialForUniqueCompositeIndex(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:  []string{"status", "due"},
		Unique: true,
	}
	idxTableDef := makeTestIndexTableDef()
	filters := []*planpb.Expr{
		makeStringEqFilterExpr(0, 3, "active"),
		makeStringEqFilterExpr(0, 4, "2026-07-02 00:00:00"),
	}

	expr, err := builder.replaceEqualCondition(idxDef, filters, []int32{0, 1}, 42, idxTableDef)
	require.NoError(t, err)

	require.NotNil(t, expr.GetF())
	require.Equal(t, "=", expr.GetF().Func.ObjName)
	assert.Equal(t, "serial", wrappedSerialFuncName(t, expr.GetF().Args[1]))
}

func TestReplaceEqualConditionTruncatesPrefixIndexLookupPart(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "status:4",
	})
	require.NoError(t, err)
	idxDef := &planpb.IndexDef{
		Parts:           []string{"status", "id"},
		Unique:          false,
		IndexAlgoParams: prefixParams,
	}
	idxTableDef := makeTestIndexTableDef()
	filters := []*planpb.Expr{makeStringEqFilterExpr(0, 1, "active")}

	expr, err := builder.replaceEqualCondition(idxDef, filters, []int32{0}, 42, idxTableDef)
	require.NoError(t, err)

	require.NotNil(t, expr.GetF())
	require.Equal(t, "prefix_eq", expr.GetF().Func.ObjName)
	require.True(t, exprContainsFuncName(expr, "substring"))
	serialFn := expr.GetF().Args[1].GetF()
	require.NotNil(t, serialFn)
	require.Len(t, serialFn.Args, 1)
	prefixArg := serialFn.Args[0].GetF()
	require.NotNil(t, prefixArg)
	if prefixArg.Func.ObjName == "cast" {
		require.Len(t, prefixArg.Args, 1)
		prefixArg = prefixArg.Args[0].GetF()
		require.NotNil(t, prefixArg)
	}
	require.Equal(t, "substring", prefixArg.Func.ObjName)
	require.Len(t, prefixArg.Args, 3)
	require.Equal(t, int64(1), prefixArg.Args[1].GetLit().GetI64Val())
	require.Equal(t, int64(4), prefixArg.Args[2].GetLit().GetI64Val())
}

func TestReplaceEqualConditionTruncatesSinglePartPrefixIndexLookup(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "status:4",
	})
	require.NoError(t, err)
	idxDef := &planpb.IndexDef{
		Parts:           []string{"status"},
		Unique:          true,
		IndexAlgoParams: prefixParams,
	}
	filters := []*planpb.Expr{makeStringEqFilterExpr(0, 1, "active")}

	expr, err := builder.replaceEqualCondition(idxDef, filters, []int32{0}, 42, makeTestIndexTableDef())
	require.NoError(t, err)

	require.NotNil(t, expr.GetF())
	require.Equal(t, "=", expr.GetF().Func.ObjName)
	require.True(t, exprContainsFuncName(expr.GetF().Args[1], "substring"))
}

func TestApplyExtraFiltersOnIndexUsesPhysicalKeyShape(t *testing.T) {
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "status:4",
	})
	require.NoError(t, err)

	tests := []struct {
		name              string
		idxDef            *planpb.IndexDef
		wantPushed        bool
		wantSerialExtract bool
	}{
		{
			name: "direct single-column unique key",
			idxDef: &planpb.IndexDef{
				Parts:  []string{"status"},
				Unique: true,
			},
			wantPushed: true,
		},
		{
			name: "serialized composite key",
			idxDef: &planpb.IndexDef{
				Parts:  []string{"status", "id"},
				Unique: true,
			},
			wantPushed:        true,
			wantSerialExtract: true,
		},
		{
			name: "lossy prefix key",
			idxDef: &planpb.IndexDef{
				Parts:           []string{"status"},
				Unique:          true,
				IndexAlgoParams: prefixParams,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
			baseTag := builder.genNewBindTag()
			indexTag := builder.genNewBindTag()
			node := &planpb.Node{
				BindingTags: []int32{baseTag},
				TableDef: &planpb.TableDef{
					Cols: []*planpb.ColDef{
						{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
						{Name: "status", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 32}},
					},
					Name2ColIndex: map[string]int32{"id": 0, "status": 1},
					Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				FilterList: []*planpb.Expr{
					makeStringEqFilterExpr(baseTag, 1, "active"),
					makeStringEqFilterExpr(baseTag, 1, "active"),
				},
			}
			idxTableNode := &planpb.Node{
				TableDef:    makeTestIndexTableDef(),
				BindingTags: []int32{indexTag},
			}

			builder.applyExtraFiltersOnIndex(tt.idxDef, node, idxTableNode, []int32{0})

			if !tt.wantPushed {
				require.Empty(t, idxTableNode.FilterList)
				return
			}
			require.Len(t, idxTableNode.FilterList, 1)
			pushed := idxTableNode.FilterList[0]
			require.Equal(t, tt.wantSerialExtract, exprContainsFuncName(pushed, "serial_extract"))
			if !tt.wantSerialExtract {
				idxCol := pushed.GetF().Args[0].GetCol()
				require.NotNil(t, idxCol)
				require.Equal(t, indexTag, idxCol.RelPos)
				require.Equal(t, int32(0), idxCol.ColPos)
			}
		})
	}
}

func TestReplaceNonEqualConditionUsesNullPropagatingSerialForNonUniqueCompositeIndexIn(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:  []string{"status", "due", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}

	expr, err := builder.replaceNonEqualCondition(idxDef, makeStringInFilterExpr(0, 3, "active", "expiring"), 42, makeTestIndexTableDef())
	require.NoError(t, err)

	require.NotNil(t, expr.GetF())
	require.Equal(t, "prefix_in", expr.GetF().Func.ObjName)
	assertListItemsWrappedBySerialFunc(t, expr.GetF().Args[1], "serial", 2)
}

func TestReplaceNonEqualConditionWrapsEachPreparedInListItemWithSerial(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:  []string{"b", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}

	expr, err := builder.replaceNonEqualCondition(idxDef, makeParamInFilterExpr(0, 1, 10), 42, makeTestIndexTableDef())
	require.NoError(t, err)

	require.NotNil(t, expr.GetF())
	require.Equal(t, "prefix_in", expr.GetF().Func.ObjName)
	assertListItemsWrappedBySerialFunc(t, expr.GetF().Args[1], "serial", 10)
	for i, item := range expr.GetF().Args[1].GetList().List {
		args := item.GetF().Args
		require.Len(t, args, 1)
		require.NotNil(t, args[0].GetP())
		assert.Equal(t, int32(i), args[0].GetP().Pos)
	}
}

func TestReplaceNonEqualConditionWidensByteStringOpenLowerBound(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:  []string{"b", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}
	tableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: catalog.CPrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: planpb.Type{Id: int32(types.T_varbinary), Width: 8}},
		},
	}
	original := makeStringInRangeFilterExpr(0, 1, "a", "b", 3)
	setIndexRangeArgumentType(original, tableDef.Cols[1].Typ)
	lookup, err := builder.replaceNonEqualCondition(idxDef, original, 42, makeTestIndexTableDef())
	require.NoError(t, err)

	require.Equal(t, uint32(3), original.GetF().Args[3].GetLit().GetU8Val())
	require.Equal(t, "prefix_in_range", lookup.GetF().Func.ObjName)
	require.Equal(t, uint32(2), lookup.GetF().Args[3].GetLit().GetU8Val())
	require.Equal(t, []int32{0}, indexOnlyResidualLeadingFilterPositions(
		idxDef, tableDef, []*planpb.Expr{original}, []int32{0}, lookup,
	))

	fixedWidth := makeIntInRangeFilterExpr(0, 1, 1, 2, 3)
	fixedLookup, err := builder.replaceNonEqualCondition(idxDef, fixedWidth, 42, makeTestIndexTableDef())
	require.NoError(t, err)
	require.Equal(t, "prefix_in_range", fixedLookup.GetF().Func.ObjName)
	require.Equal(t, uint32(3), fixedLookup.GetF().Args[3].GetLit().GetU8Val())
}

func TestIndexOnlyResidualDetectsNestedByteStringPrefixLookup(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:  []string{"b", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}
	tableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: catalog.CPrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: planpb.Type{Id: int32(types.T_varbinary), Width: 8}},
		},
	}
	original := makeOrFilterExpr(
		makeStringBetweenFilterExpr(0, 1, "\x01", "\x01"),
		makeStringInFilterExpr(0, 1, "", "\x00"),
	)
	setIndexFilterArgumentType(original.GetF().Args[0], tableDef.Cols[1].Typ)
	setIndexFilterArgumentType(original.GetF().Args[1], tableDef.Cols[1].Typ)
	lookup, err := builder.replaceNonEqualCondition(idxDef, original, 42, makeTestIndexTableDef())
	require.NoError(t, err)

	require.Equal(t, "or", lookup.GetF().Func.ObjName)
	require.Equal(t, "prefix_between", lookup.GetF().Args[0].GetF().Func.ObjName)
	require.Equal(t, "prefix_in", lookup.GetF().Args[1].GetF().Func.ObjName)
	require.Equal(t, []int32{0}, indexOnlyResidualLeadingFilterPositions(
		idxDef, tableDef, []*planpb.Expr{original}, []int32{0}, lookup,
	))
}

func TestIndexOnlyResidualLeadingFilterPositionsAreMinimal(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:  []string{"first", "last", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}
	tableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: catalog.CPrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "first", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "last", Typ: planpb.Type{Id: int32(types.T_varbinary), Width: 8}},
		},
	}
	firstLiteral := makeEqFilterExpr(1)
	firstLiteral.GetF().Args[0].GetCol().RelPos = 0
	lastLiteral := makeStringEqFilterExpr(0, 2, "\x00")
	setIndexFilterArgumentType(lastLiteral, tableDef.Cols[2].Typ)
	filters := []*planpb.Expr{firstLiteral, lastLiteral}
	lookup, err := builder.replaceEqualCondition(idxDef, filters, []int32{0, 1}, 42, makeTestIndexTableDef())
	require.NoError(t, err)

	require.Equal(t, []int32{1}, indexOnlyResidualLeadingFilterPositions(
		idxDef, tableDef, filters, []int32{0, 1}, lookup,
	))

	firstPrepared := makeParamEqFilterExpr(0, 1, 0)
	filters[0] = firstPrepared
	lookup, err = builder.replaceEqualCondition(idxDef, filters, []int32{0, 1}, 42, makeTestIndexTableDef())
	require.NoError(t, err)
	require.Equal(t, []int32{1}, indexOnlyResidualLeadingFilterPositions(
		idxDef, tableDef, filters, []int32{0, 1}, lookup,
	))

	tableDef.Cols[1].Typ = planpb.Type{Id: int32(types.T_varbinary), Width: 8}
	tableDef.Cols[2].Typ = planpb.Type{Id: int32(types.T_int64)}
	firstByteString := makeStringEqFilterExpr(0, 1, "\x00")
	setIndexFilterArgumentType(firstByteString, tableDef.Cols[1].Typ)
	lastFixedWidth := makeEqFilterExpr(2)
	lastFixedWidth.GetF().Args[0].GetCol().RelPos = 0
	filters = []*planpb.Expr{firstByteString, lastFixedWidth}
	lookup, err = builder.replaceEqualCondition(idxDef, filters, []int32{0, 1}, 42, makeTestIndexTableDef())
	require.NoError(t, err)
	require.Empty(t, indexOnlyResidualLeadingFilterPositions(
		idxDef, tableDef, filters, []int32{0, 1}, lookup,
	))
}

func TestTryIndexOnlyScanRejectsLossyPrefixIndex(t *testing.T) {
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "status:4",
	})
	require.NoError(t, err)

	tests := []struct {
		name            string
		indexAlgoParams string
		wantIndexOnly   bool
	}{
		{name: "full value index", wantIndexOnly: true},
		{name: "prefix index", indexAlgoParams: prefixParams, wantIndexOnly: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
			ctx := NewBindContext(builder, nil)
			bindTag := builder.genNewBindTag()
			idxDef := &planpb.IndexDef{
				IndexName:       "idx_status_id",
				IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
				IndexAlgoParams: tt.indexAlgoParams,
				IndexTableName:  "__mo_idx_status_id",
				Parts:           []string{"status", "id"},
				Unique:          false,
				TableExist:      true,
			}
			registerMockIndexTable(t, builder, idxDef.IndexTableName)
			node := &planpb.Node{
				NodeType:    planpb.Node_TABLE_SCAN,
				ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
				BindingTags: []int32{bindTag},
				TableDef: &planpb.TableDef{
					Name: "t",
					Cols: []*planpb.ColDef{
						{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
						{Name: "status", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 32}},
					},
					Name2ColIndex: map[string]int32{"id": 0, "status": 1},
					Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				Stats:      &planpb.Stats{TableCnt: 100, Outcnt: 1, Selectivity: 0.01, Cost: 100},
				FilterList: []*planpb.Expr{makeStringEqFilterExpr(bindTag, 1, "active")},
			}
			scanID := builder.appendNode(node, ctx)

			idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], map[[2]int32]int{{bindTag, 1}: 1}, map[[2]int32]*planpb.Expr{}, &Snapshot{})
			if tt.wantIndexOnly {
				require.NotEqual(t, int32(-1), idxNodeID)
			} else {
				require.Equal(t, int32(-1), idxNodeID)
			}
		})
	}
}

func TestApplyIndicesForFiltersUsesIndexJoinForPrefixIndex(t *testing.T) {
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "status:4",
	})
	require.NoError(t, err)

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	bindTag := builder.genNewBindTag()
	idxDef := &planpb.IndexDef{
		IndexName:       "idx_status_id",
		IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
		IndexAlgoParams: prefixParams,
		IndexTableName:  "__mo_idx_status_id",
		Parts:           []string{"status", "id"},
		Unique:          false,
		TableExist:      true,
	}
	registerMockIndexTable(t, builder, idxDef.IndexTableName)
	node := &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name: "t",
			Cols: []*planpb.ColDef{
				{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "status", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 32}},
			},
			Name2ColIndex: map[string]int32{"id": 0, "status": 1},
			Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			Indexes:       []*planpb.IndexDef{nil, idxDef},
		},
		Stats:      &planpb.Stats{TableCnt: 100, Outcnt: 1, Selectivity: 0.01, Cost: 100},
		FilterList: []*planpb.Expr{makeStringEqFilterExpr(bindTag, 1, "active")},
	}
	scanID := builder.appendNode(node, ctx)

	resultID := builder.applyIndicesForFilters(scanID, builder.qry.Nodes[scanID], map[[2]int32]int{{bindTag, 1}: 1}, map[[2]int32]*planpb.Expr{})
	require.NotEqual(t, scanID, resultID)

	indexJoin := builder.qry.Nodes[resultID]
	require.Equal(t, planpb.Node_JOIN, indexJoin.NodeType)
	require.Equal(t, planpb.Node_INDEX, indexJoin.JoinType)
	require.Equal(t, scanID, indexJoin.Children[0])
	require.Len(t, builder.qry.Nodes[scanID].FilterList, 1)
	require.Equal(t, "=", builder.qry.Nodes[scanID].FilterList[0].GetF().Func.ObjName)

	indexScan := builder.qry.Nodes[indexJoin.Children[1]]
	require.True(t, indexScan.IndexScanInfo.IsIndexScan)
	require.Equal(t, idxDef.IndexName, indexScan.IndexScanInfo.IndexName)
}

func TestApplyIndicesForFiltersIgnoresVisibilityMetadata(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	bindTag := builder.genNewBindTag()
	idxDef := &planpb.IndexDef{
		IndexName:      "idx_status_id",
		IndexTableName: "__mo_idx_status_id",
		Parts:          []string{"status", "id"},
		TableExist:     true,
		Visible:        false,
	}
	registerMockIndexTable(t, builder, idxDef.IndexTableName)
	node := &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name: "t",
			Cols: []*planpb.ColDef{
				{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "status", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 32}},
			},
			Name2ColIndex: map[string]int32{"id": 0, "status": 1},
			Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			Indexes:       []*planpb.IndexDef{idxDef},
		},
		Stats:      &planpb.Stats{TableCnt: 100, Outcnt: 1, Selectivity: 0.01, Cost: 100},
		FilterList: []*planpb.Expr{makeStringEqFilterExpr(bindTag, 1, "active")},
	}
	scanID := builder.appendNode(node, ctx)

	resultID := builder.applyIndicesForFiltersRegularIndex(scanID, builder.qry.Nodes[scanID],
		map[[2]int32]int{{bindTag, 1}: 1}, map[[2]int32]*planpb.Expr{})
	require.NotEqual(t, scanID, resultID)
}

func TestTryIndexOnlyScanUsesComparisonNullSemanticsAndMinimalResiduals(t *testing.T) {
	tests := []struct {
		name         string
		makeFilter   func(relPos int32) *planpb.Expr
		lookupFunc   string
		residualFunc string
	}{
		{
			name: "prepared equality",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeParamEqFilterExpr(relPos, 1, 0)
			},
			lookupFunc: "prefix_eq",
		},
		{
			name: "prepared in list",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeParamInFilterExpr(relPos, 1, 2)
			},
			lookupFunc: "prefix_in",
		},
		{
			name: "literal null equality",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeNullEqFilterExpr(relPos, 1)
			},
			lookupFunc: "prefix_eq",
		},
		{
			name: "literal null in list",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeIntInFilterExprWithNull(relPos, 1)
			},
			lookupFunc: "prefix_in",
		},
		{
			name: "prepared between",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeParamBetweenFilterExpr(relPos, 1, 0, 1)
			},
			lookupFunc: "prefix_between",
		},
		{
			name: "nullable strict upper bound literal",
			makeFilter: func(relPos int32) *planpb.Expr {
				filter := makeRangeFilterExpr(relPos, 1, "<", 10)
				filter.Typ = planpb.Type{Id: int32(types.T_bool)}
				filter.GetF().Args[0].Typ = planpb.Type{Id: int32(types.T_int32)}
				filter.GetF().Args[1].Typ = planpb.Type{Id: int32(types.T_int32)}
				return filter
			},
			lookupFunc:   "<",
			residualFunc: "<",
		},
		{
			name: "nullable strict upper bound prepared",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeParamRangeFilterExpr(relPos, 1, "<", 0)
			},
			lookupFunc:   "<",
			residualFunc: "<",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
			ctx := NewBindContext(builder, nil)
			bindTag := builder.genNewBindTag()
			idxDef := &planpb.IndexDef{
				IndexName:      "idx_status_id",
				IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
				IndexTableName: "__mo_idx_status_id",
				Parts:          []string{"status", "id"},
				Unique:         false,
				TableExist:     true,
			}
			registerMockIndexTable(t, builder, idxDef.IndexTableName)
			node := &planpb.Node{
				NodeType:    planpb.Node_TABLE_SCAN,
				ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
				BindingTags: []int32{bindTag},
				TableDef: &planpb.TableDef{
					Name: "t",
					Cols: []*planpb.ColDef{
						{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
						{Name: "status", Typ: planpb.Type{Id: int32(types.T_int32)}},
					},
					Name2ColIndex: map[string]int32{
						"id":     0,
						"status": 1,
					},
					Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				Stats:      &planpb.Stats{TableCnt: 100, Outcnt: 1, Selectivity: 0.01, Cost: 100},
				FilterList: []*planpb.Expr{tt.makeFilter(bindTag)},
			}
			scanID := builder.appendNode(node, ctx)

			idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], map[[2]int32]int{{bindTag, 1}: 1}, map[[2]int32]*planpb.Expr{}, &Snapshot{})
			require.NotEqual(t, int32(-1), idxNodeID)

			idxNode := builder.qry.Nodes[idxNodeID]
			require.Equal(t, 1, countExprFunctionCalls(idxNode.FilterList[:1], tt.lookupFunc))
			require.Zero(t, countExprFunctionCalls(idxNode.FilterList[:1], "isnotnull"))
			require.NotZero(t, countExprFunctionCalls(idxNode.FilterList[:1], "serial"))
			require.Zero(t, countExprFunctionCalls(idxNode.FilterList[:1], "serial_full"))
			if tt.residualFunc == "" {
				require.Len(t, idxNode.FilterList, 1)
				return
			}
			require.Len(t, idxNode.FilterList, 2)
			residual := idxNode.FilterList[1].GetF()
			require.NotNil(t, residual)
			require.Equal(t, tt.residualFunc, residual.Func.ObjName)
			require.Equal(t, "serial_extract", wrappedSerialFuncName(t, residual.Args[0]))
		})
	}
}

func TestIndexFilterNeedsDecodedNullResidual(t *testing.T) {
	makeLiteralRange := func(op string, constOnLeft, notNullable bool) *planpb.Expr {
		filter := makeRangeFilterExpr(7, 1, op, 10)
		filter.Typ = planpb.Type{Id: int32(types.T_bool)}
		filter.GetF().Args[0].Typ = planpb.Type{Id: int32(types.T_int64), NotNullable: notNullable}
		filter.GetF().Args[1].Typ = planpb.Type{Id: int32(types.T_int64), NotNullable: true}
		if constOnLeft {
			filter.GetF().Args[0], filter.GetF().Args[1] = filter.GetF().Args[1], filter.GetF().Args[0]
		}
		return filter
	}
	makeOr := func(args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "or"},
				Args: args,
			}},
		}
	}

	tests := []struct {
		name string
		expr *planpb.Expr
		want bool
	}{
		{name: "nullable column strict upper bound", expr: makeLiteralRange("<", false, false), want: true},
		{name: "constant-left strict upper bound", expr: makeLiteralRange(">", true, false), want: true},
		{name: "non-null column strict upper bound", expr: makeLiteralRange("<", false, true)},
		{name: "nullable column inclusive lower bound", expr: makeLiteralRange(">=", false, false)},
		{name: "prepared bound", expr: makeParamRangeFilterExpr(7, 1, "<", 0), want: true},
		{name: "prepared equality uses NULL-propagating serialization", expr: makeParamEqFilterExpr(7, 1, 0)},
		{name: "prepared between uses NULL-propagating serialization", expr: makeParamBetweenFilterExpr(7, 1, 0, 1)},
		{name: "prepared IN ignores NULL access needles", expr: makeParamInFilterExpr(7, 1, 2)},
		{
			name: "or with strict upper arm",
			expr: makeOr(
				makeLiteralRange("<", false, false),
				makeLiteralRange(">=", false, false),
			),
			want: true,
		},
		{
			name: "or with lower-bound arms",
			expr: makeOr(
				makeLiteralRange(">=", false, false),
				makeLiteralRange("<=", true, false),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, indexFilterNeedsDecodedNullResidual(tt.expr))
		})
	}
}

func TestNullableStrictUpperBoundRegularIndexPlans(t *testing.T) {
	t.Run("one-part covering limit scan keeps decoded residual", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)

		queryPlan, err := runOneStmt(mock, t, "select id, a from index_hint_t force index(idx_a) where a < 10 limit 1")
		require.NoError(t, err)
		indexScan := findFirstIndexScanNode(queryPlan)
		require.NotNil(t, indexScan)
		require.Equal(t, "idx_a", indexScan.IndexScanInfo.IndexName)
		require.Len(t, indexScan.IndexScanInfo.Parts, 2)
		require.Equal(t, "a", indexScan.IndexScanInfo.Parts[0])
		require.True(t, catalog.IsAlias(indexScan.IndexScanInfo.Parts[1]))
		require.Len(t, indexScan.FilterList, 2)
		require.Equal(t, "<", indexScan.FilterList[0].GetF().Func.ObjName)
		require.Equal(t, "<", indexScan.FilterList[1].GetF().Func.ObjName)
		require.Equal(t, "serial_extract", wrappedSerialFuncName(t, indexScan.FilterList[1].GetF().Args[0]))
		require.NotNil(t, indexScan.Limit)
	})

	t.Run("safe or keeps decoded residual", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)

		queryPlan, err := runOneStmt(mock, t, "select id from index_hint_t force index(idx_a) where a < 10 or a >= 100")
		require.NoError(t, err)
		indexScan := findFirstIndexScanNode(queryPlan)
		require.NotNil(t, indexScan)
		require.Len(t, indexScan.FilterList, 2)
		require.Equal(t, "or", indexScan.FilterList[0].GetF().Func.ObjName)

		residual := indexScan.FilterList[1].GetF()
		require.Equal(t, "or", residual.Func.ObjName)
		require.Len(t, residual.Args, 2)
		for _, arm := range residual.Args {
			require.Equal(t, "serial_extract", wrappedSerialFuncName(t, arm.GetF().Args[0]))
		}
	})

	t.Run("non-nullable strict upper bound skips residual", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)
		mock.ctxt.tables["index_hint_t"].Cols[1].Typ.NotNullable = true

		queryPlan, err := runOneStmt(mock, t, "select id from index_hint_t force index(idx_a) where a < 10")
		require.NoError(t, err)
		indexScan := findFirstIndexScanNode(queryPlan)
		require.NotNil(t, indexScan)
		require.Len(t, indexScan.FilterList, 1)
	})

	t.Run("backfill join keeps base residual", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addIndexHintChoiceTableForTest(mock)
		tableDef := mock.ctxt.tables["index_hint_t"]
		payloadPos := int32(len(tableDef.Cols))
		tableDef.Cols = append(tableDef.Cols, &planpb.ColDef{
			ColId: 4, Name: "payload", OriginName: "payload",
			Typ:     planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			Default: &planpb.Default{NullAbility: true},
		})
		tableDef.Name2ColIndex["payload"] = payloadPos

		queryPlan, err := runOneStmt(mock, t, "select payload from index_hint_t where a < 10")
		require.NoError(t, err)
		require.NotEmpty(t, findFirstIndexScanName(queryPlan))
		require.True(t, planHasIndexJoin(queryPlan))
		var baseScan *planpb.Node
		for _, node := range queryPlan.GetQuery().Nodes {
			if node.NodeType == planpb.Node_TABLE_SCAN && !node.IndexScanInfo.IsIndexScan && node.TableDef != nil && node.TableDef.Name == "index_hint_t" {
				baseScan = node
				break
			}
		}
		require.NotNil(t, baseScan)
		require.Len(t, baseScan.FilterList, 1)
		require.Equal(t, "<", baseScan.FilterList[0].GetF().Func.ObjName)
	})

	for _, sql := range []string{
		"select id from index_hint_t where a <= 10",
		"select id from index_hint_t where a > 10",
	} {
		t.Run("unsafe prefix comparison stays on base scan "+sql, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)

			queryPlan, err := runOneStmt(mock, t, sql)
			require.NoError(t, err)
			require.Empty(t, findFirstIndexScanName(queryPlan))
		})
	}
}

func TestTryIndexOnlyScanPreservesVarcharResidualForPrefixPredicates(t *testing.T) {
	tests := []struct {
		name       string
		makeFilter func(relPos int32) *planpb.Expr
		lookupFunc string
		residual   string
	}{
		{
			name: "literal equality",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringEqFilterExpr(relPos, 1, "active")
			},
			lookupFunc: "prefix_eq",
			residual:   "=",
		},
		{
			name: "literal in list",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringInFilterExpr(relPos, 1, "active", "expired")
			},
			lookupFunc: "prefix_in",
			residual:   "in",
		},
		{
			name: "literal between",
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringBetweenFilterExpr(relPos, 1, "active", "expired")
			},
			lookupFunc: "prefix_between",
			residual:   "between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
			ctx := NewBindContext(builder, nil)
			bindTag := builder.genNewBindTag()
			idxDef := &planpb.IndexDef{
				IndexName:      "idx_status_id",
				IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
				IndexTableName: "__mo_idx_status_id",
				Parts:          []string{"status", "id"},
				Unique:         false,
				TableExist:     true,
			}
			registerMockIndexTable(t, builder, idxDef.IndexTableName)
			node := &planpb.Node{
				NodeType:    planpb.Node_TABLE_SCAN,
				ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
				BindingTags: []int32{bindTag},
				TableDef: &planpb.TableDef{
					Name: "t",
					Cols: []*planpb.ColDef{
						{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
						{Name: "status", Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
					},
					Name2ColIndex: map[string]int32{
						"id":     0,
						"status": 1,
					},
					Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				Stats:      &planpb.Stats{TableCnt: 100, Outcnt: 1, Selectivity: 0.01, Cost: 100},
				FilterList: []*planpb.Expr{tt.makeFilter(bindTag)},
			}
			scanID := builder.appendNode(node, ctx)

			idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], map[[2]int32]int{{bindTag, 1}: 1}, map[[2]int32]*planpb.Expr{}, &Snapshot{})
			require.NotEqual(t, int32(-1), idxNodeID)

			idxNode := builder.qry.Nodes[idxNodeID]
			require.Equal(t, tt.lookupFunc, idxNode.FilterList[0].GetF().Func.ObjName)
			if tt.residual == "" {
				require.Len(t, idxNode.FilterList, 1)
				return
			}
			require.Len(t, idxNode.FilterList, 2)
			require.Equal(t, tt.residual, idxNode.FilterList[1].GetF().Func.ObjName)
		})
	}
}

func TestTryIndexOnlyScanHandlesByteStringPrefixLookups(t *testing.T) {
	tests := []struct {
		name       string
		typ        types.T
		makeFilter func(relPos int32) *planpb.Expr
		lookupFunc string
		residual   string
	}{
		{
			name: "varbinary equality",
			typ:  types.T_varbinary,
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringEqFilterExpr(relPos, 1, "\x00")
			},
			lookupFunc: "prefix_eq",
			residual:   "=",
		},
		{
			name: "varbinary in",
			typ:  types.T_varbinary,
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringInFilterExpr(relPos, 1, "", "\x00")
			},
			lookupFunc: "prefix_in",
			residual:   "in",
		},
		{
			name: "varbinary between",
			typ:  types.T_varbinary,
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringBetweenFilterExpr(relPos, 1, "\x00", "\x00\x01")
			},
			lookupFunc: "prefix_between",
			residual:   "between",
		},
		{
			name: "binary equality",
			typ:  types.T_binary,
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringEqFilterExpr(relPos, 1, "\x00")
			},
			lookupFunc: "prefix_eq",
			residual:   "=",
		},
		{
			name: "binary in",
			typ:  types.T_binary,
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringInFilterExpr(relPos, 1, "", "\x00")
			},
			lookupFunc: "prefix_in",
			residual:   "in",
		},
		{
			name: "binary between",
			typ:  types.T_binary,
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringBetweenFilterExpr(relPos, 1, "\x00", "\x00\x01")
			},
			lookupFunc: "prefix_between",
			residual:   "between",
		},
		{
			name: "varchar equality with encoded terminator collision",
			typ:  types.T_varchar,
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringEqFilterExpr(relPos, 1, "a")
			},
			lookupFunc: "prefix_eq",
			residual:   "=",
		},
		{
			name: "char in with encoded terminator collision",
			typ:  types.T_char,
			makeFilter: func(relPos int32) *planpb.Expr {
				return makeStringInFilterExpr(relPos, 1, "a", "b")
			},
			lookupFunc: "prefix_in",
			residual:   "in",
		},
		{
			name: "int64 equality fixed-width control",
			typ:  types.T_int64,
			makeFilter: func(relPos int32) *planpb.Expr {
				expr := makeEqFilterExpr(1)
				expr.GetF().Args[0].GetCol().RelPos = relPos
				return expr
			},
			lookupFunc: "prefix_eq",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
			ctx := NewBindContext(builder, nil)
			bindTag := builder.genNewBindTag()
			idxDef := &planpb.IndexDef{
				IndexName:      "idx_b_id",
				IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
				IndexTableName: "__mo_idx_b_id",
				Parts:          []string{"b", "id"},
				Unique:         false,
				TableExist:     true,
			}
			registerMockIndexTable(t, builder, idxDef.IndexTableName)
			filter := tt.makeFilter(bindTag)
			binaryType := planpb.Type{Id: int32(tt.typ), Width: 8}
			setIndexFilterArgumentType(filter, binaryType)
			node := &planpb.Node{
				NodeType:    planpb.Node_TABLE_SCAN,
				ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
				BindingTags: []int32{bindTag},
				TableDef: &planpb.TableDef{
					Name: "t",
					Cols: []*planpb.ColDef{
						{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
						{Name: "b", Typ: binaryType},
					},
					Name2ColIndex: map[string]int32{"id": 0, "b": 1},
					Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				Stats:      &planpb.Stats{TableCnt: 100, Outcnt: 1, Selectivity: 0.01, Cost: 100},
				FilterList: []*planpb.Expr{filter},
			}
			scanID := builder.appendNode(node, ctx)

			idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID], map[[2]int32]int{{bindTag, 1}: 1}, map[[2]int32]*planpb.Expr{}, &Snapshot{})
			require.NotEqual(t, int32(-1), idxNodeID)

			idxNode := builder.qry.Nodes[idxNodeID]
			require.Equal(t, tt.lookupFunc, idxNode.FilterList[0].GetF().Func.ObjName)
			if tt.residual == "" {
				require.Len(t, idxNode.FilterList, 1)
				return
			}
			require.Len(t, idxNode.FilterList, 2)
			require.Equal(t, tt.residual, idxNode.FilterList[1].GetF().Func.ObjName)
			require.Equal(t, "serial_extract", wrappedSerialFuncName(t, idxNode.FilterList[1].GetF().Args[0]))
			require.Equal(t, int32(tt.typ), idxNode.FilterList[1].GetF().Args[0].Typ.Id)
		})
	}
}

func TestIndexOnlyResidualLeadingFilterPositionsUsesTrailingPrefixPart(t *testing.T) {
	idxDef := &planpb.IndexDef{
		Parts:  []string{"v", "n", "id"},
		Unique: false,
	}
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
		{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 16}},
		{Name: "n", Typ: planpb.Type{Id: int32(types.T_int64)}},
	}}
	filters := []*planpb.Expr{
		makeStringEqFilterExpr(0, 1, "a"),
		makeEqFilterExpr(2),
	}
	lookupFilter := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "prefix_eq"},
	}}}

	// A byte-string component before another encoded component is delimited by
	// that component's type byte. Only the trailing component can collide with
	// the appended primary-key suffix, so a fixed-width trailing part needs no
	// residual recheck.
	require.Empty(t, indexOnlyResidualLeadingFilterPositions(
		idxDef, tableDef, filters, []int32{0, 1}, lookupFilter,
	))
	require.Equal(t, []int32{0}, indexOnlyResidualLeadingFilterPositions(
		idxDef, tableDef, filters, []int32{1, 0}, lookupFilter,
	))
}

func TestIndexOnlyResidualLeadingFilterPositionsRecognizesNestedPrefixRange(t *testing.T) {
	idxDef := &planpb.IndexDef{Parts: []string{"v", "id"}, Unique: false}
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
		{Name: "v", Typ: planpb.Type{Id: int32(types.T_varbinary), Width: 16}},
	}}
	filter := makeStringBetweenFilterExpr(0, 1, "\x00", "\x00")
	prefixRange := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "prefix_in_range"},
	}}}
	lookupFilter := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "or"},
		Args: []*planpb.Expr{prefixRange},
	}}}

	require.Equal(t, []int32{0}, indexOnlyResidualLeadingFilterPositions(
		idxDef, tableDef, []*planpb.Expr{filter}, []int32{0}, lookupFilter,
	))
}

func TestReplaceRangePairCondition_UsesPrefixBetweenForSecondaryIndex(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	idxDef := &planpb.IndexDef{
		Parts:  []string{"price", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}
	filters := []*planpb.Expr{
		makeRangeFilterExpr(bindTag, 1, ">=", 99),
		makeRangeFilterExpr(bindTag, 1, "<=", 299),
	}
	filters[0].Selectivity = 0.3
	filters[1].Selectivity = 0.4

	idxTableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{
				Name: catalog.IndexTableIndexColName,
				Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			},
			{
				Name: catalog.IndexTablePrimaryColName,
				Typ:  planpb.Type{Id: int32(types.T_uint64)},
			},
		},
	}

	expr, err := builder.replaceRangePairCondition(idxDef, filters, []int32{0, 1}, 42, idxTableDef)
	require.NoError(t, err)
	require.NotNil(t, expr.GetF())
	require.Equal(t, "prefix_between", expr.GetF().Func.ObjName)
	assert.Equal(t, "serial", wrappedSerialFuncName(t, expr.GetF().Args[1]))
	assert.Equal(t, "serial", wrappedSerialFuncName(t, expr.GetF().Args[2]))
	require.InDelta(t, 0.12, expr.Selectivity, 1e-9)
}

func TestTryIndexOnlyScanCoalescesRangePair(t *testing.T) {
	makeScan := func(t *testing.T, columnType planpb.Type, filters []*planpb.Expr) (*QueryBuilder, int32, *planpb.IndexDef) {
		t.Helper()
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		ctx := NewBindContext(builder, nil)
		bindTag := builder.genNewBindTag()
		idxDef := &planpb.IndexDef{
			IndexName:      "idx_price",
			IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
			IndexTableName: "__mo_idx_price",
			Parts:          []string{"price", "id"},
			Unique:         false,
			TableExist:     true,
		}
		registerMockIndexTable(t, builder, idxDef.IndexTableName)
		for _, filter := range filters {
			filter.GetF().Args[0].GetCol().RelPos = bindTag
			setIndexRangeArgumentType(filter, columnType)
			filter.Selectivity = 0.2
		}
		node := &planpb.Node{
			NodeType:    planpb.Node_TABLE_SCAN,
			ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
			BindingTags: []int32{bindTag},
			TableDef: &planpb.TableDef{
				Name: "t",
				Cols: []*planpb.ColDef{
					{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
					{Name: "price", Typ: columnType},
				},
				Name2ColIndex: map[string]int32{"id": 0, "price": 1},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
			Stats:      &planpb.Stats{TableCnt: 100, Outcnt: 4, Selectivity: 0.04, Cost: 100},
			FilterList: filters,
		}
		return builder, builder.appendNode(node, ctx), idxDef
	}

	t.Run("closed fixed-width bounds become one prefix_between", func(t *testing.T) {
		filters := []*planpb.Expr{
			makeRangeFilterExpr(0, 1, ">=", 20),
			makeRangeFilterExpr(0, 1, "<=", 50),
		}
		builder, scanID, idxDef := makeScan(t, planpb.Type{Id: int32(types.T_int64)}, filters)
		bindTag := builder.qry.Nodes[scanID].BindingTags[0]

		idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID],
			map[[2]int32]int{{bindTag, 1}: 1}, map[[2]int32]*planpb.Expr{}, &Snapshot{})
		require.NotEqual(t, int32(-1), idxNodeID)
		indexScan := builder.qry.Nodes[idxNodeID]
		require.Len(t, indexScan.FilterList, 1)
		require.Equal(t, "prefix_between", indexScan.FilterList[0].GetF().Func.ObjName)
	})

	t.Run("byte-string bounds retain exact residuals", func(t *testing.T) {
		filters := []*planpb.Expr{
			makeStringRangeFilterExpr(0, 1, ">", "a"),
			makeStringRangeFilterExpr(0, 1, "<=", "b"),
		}
		builder, scanID, idxDef := makeScan(t, planpb.Type{Id: int32(types.T_varbinary), Width: 8}, filters)
		bindTag := builder.qry.Nodes[scanID].BindingTags[0]

		idxNodeID := builder.tryIndexOnlyScan(idxDef, builder.qry.Nodes[scanID],
			map[[2]int32]int{{bindTag, 1}: 1}, map[[2]int32]*planpb.Expr{}, &Snapshot{})
		require.NotEqual(t, int32(-1), idxNodeID)
		indexScan := builder.qry.Nodes[idxNodeID]
		require.Len(t, indexScan.FilterList, 3)
		require.Equal(t, "prefix_in_range", indexScan.FilterList[0].GetF().Func.ObjName)
		require.Equal(t, ">", indexScan.FilterList[1].GetF().Func.ObjName)
		require.Equal(t, "<=", indexScan.FilterList[2].GetF().Func.ObjName)
	})
}

func TestReplaceRangePairConditionWidensByteStringOpenLowerBound(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	idxDef := &planpb.IndexDef{
		Parts:  []string{"b", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}
	idxTableDef := makeTestIndexTableDef()
	byteStringFilters := []*planpb.Expr{
		makeStringRangeFilterExpr(0, 1, ">", "a"),
		makeStringRangeFilterExpr(0, 1, "<=", "b"),
	}
	setIndexRangeArgumentType(byteStringFilters[0], planpb.Type{Id: int32(types.T_varbinary), Width: 8})
	setIndexRangeArgumentType(byteStringFilters[1], planpb.Type{Id: int32(types.T_varbinary), Width: 8})

	byteStringLookup, err := builder.replaceRangePairCondition(idxDef, byteStringFilters, []int32{0, 1}, 42, idxTableDef)
	require.NoError(t, err)
	require.Equal(t, "prefix_in_range", byteStringLookup.GetF().Func.ObjName)
	require.Equal(t, uint32(0), byteStringLookup.GetF().Args[3].GetLit().GetU8Val())

	fixedWidthFilters := []*planpb.Expr{
		makeRangeFilterExpr(0, 1, ">", 1),
		makeRangeFilterExpr(0, 1, "<=", 2),
	}
	fixedWidthLookup, err := builder.replaceRangePairCondition(idxDef, fixedWidthFilters, []int32{0, 1}, 42, idxTableDef)
	require.NoError(t, err)
	require.Equal(t, "prefix_in_range", fixedWidthLookup.GetF().Func.ObjName)
	require.Equal(t, uint32(1), fixedWidthLookup.GetF().Args[3].GetLit().GetU8Val())
}

func TestIndexRangeSerializationNormalizesDecimalBounds(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	idxDef := &planpb.IndexDef{
		Parts:  []string{"price", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
		Unique: false,
	}
	idxTableDef := makeTestIndexTableDef()
	columnType := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	higherScaleType := planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 6}

	t.Run("closed pair", func(t *testing.T) {
		filters := []*planpb.Expr{
			makeDecimalRangeFilterExpr(t, bindTag, 1, ">=", "10.250000", columnType, higherScaleType),
			makeDecimalRangeFilterExpr(t, bindTag, 1, "<=", "15.750000", columnType, higherScaleType),
		}

		expr, err := builder.replaceRangePairCondition(idxDef, filters, []int32{0, 1}, 42, idxTableDef)
		require.NoError(t, err)

		require.Equal(t, "prefix_between", expr.GetF().Func.ObjName)
		requireSerializedRangeBoundType(t, expr.GetF().Args[1], columnType, true)
		requireSerializedRangeBoundType(t, expr.GetF().Args[2], columnType, true)
	})

	t.Run("open pair", func(t *testing.T) {
		filters := []*planpb.Expr{
			makeDecimalRangeFilterExpr(t, bindTag, 1, ">", "10.250000", columnType, higherScaleType),
			makeDecimalRangeFilterExpr(t, bindTag, 1, "<", "15.750000", columnType, higherScaleType),
		}

		expr, err := builder.replaceRangePairCondition(idxDef, filters, []int32{0, 1}, 42, idxTableDef)
		require.NoError(t, err)

		require.Equal(t, "prefix_in_range", expr.GetF().Func.ObjName)
		requireSerializedRangeBoundType(t, expr.GetF().Args[1], columnType, true)
		requireSerializedRangeBoundType(t, expr.GetF().Args[2], columnType, true)
	})

	for _, tc := range []struct {
		name string
		op   string
	}{
		{name: "lower only", op: ">="},
		{name: "upper only", op: "<"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := makeDecimalRangeFilterExpr(t, bindTag, 1, tc.op, "10.250000", columnType, higherScaleType)

			expr, err := builder.replaceNonEqualCondition(idxDef, filter, 42, idxTableDef)
			require.NoError(t, err)

			require.Equal(t, tc.op, expr.GetF().Func.ObjName)
			requireSerializedRangeBoundType(t, expr.GetF().Args[1], columnType, true)
		})
	}

	t.Run("in range", func(t *testing.T) {
		lower := makeDecimalRangeFilterExpr(t, bindTag, 1, ">", "10.250000", columnType, higherScaleType)
		upper := makeDecimalRangeFilterExpr(t, bindTag, 1, "<", "15.750000", columnType, higherScaleType)
		filter := &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "in_range"},
				Args: []*planpb.Expr{
					DeepCopyExpr(lower.GetF().Args[0]),
					DeepCopyExpr(lower.GetF().Args[1]),
					DeepCopyExpr(upper.GetF().Args[1]),
					MakePlan2Uint8ConstExprWithType(3),
				},
			}},
		}

		expr, err := builder.replaceNonEqualCondition(idxDef, filter, 42, idxTableDef)
		require.NoError(t, err)

		require.Equal(t, "prefix_in_range", expr.GetF().Func.ObjName)
		requireSerializedRangeBoundType(t, expr.GetF().Args[1], columnType, true)
		requireSerializedRangeBoundType(t, expr.GetF().Args[2], columnType, true)
	})

	t.Run("equal scale decimal remains direct", func(t *testing.T) {
		filters := []*planpb.Expr{
			makeDecimalRangeFilterExpr(t, bindTag, 1, ">=", "10.25", columnType, columnType),
			makeDecimalRangeFilterExpr(t, bindTag, 1, "<=", "15.75", columnType, columnType),
		}

		expr, err := builder.replaceRangePairCondition(idxDef, filters, []int32{0, 1}, 42, idxTableDef)
		require.NoError(t, err)

		requireSerializedRangeBoundType(t, expr.GetF().Args[1], columnType, false)
		requireSerializedRangeBoundType(t, expr.GetF().Args[2], columnType, false)
	})

	t.Run("non decimal remains direct", func(t *testing.T) {
		intType := planpb.Type{Id: int32(types.T_int64)}
		filters := []*planpb.Expr{
			makeTypedInt64RangeFilterExpr(bindTag, 1, ">=", 10, intType),
			makeTypedInt64RangeFilterExpr(bindTag, 1, "<=", 15, intType),
		}

		expr, err := builder.replaceRangePairCondition(idxDef, filters, []int32{0, 1}, 42, idxTableDef)
		require.NoError(t, err)

		requireSerializedRangeBoundType(t, expr.GetF().Args[1], intType, false)
		requireSerializedRangeBoundType(t, expr.GetF().Args[2], intType, false)
	})

	t.Run("non representable decimal follows index key encoding", func(t *testing.T) {
		filters := []*planpb.Expr{
			makeDecimalRangeFilterExpr(t, bindTag, 1, ">", "10.255000", columnType, higherScaleType),
			makeDecimalRangeFilterExpr(t, bindTag, 1, "<=", "15.755000", columnType, higherScaleType),
		}
		makeNode := func(filterList []*planpb.Expr) *planpb.Node {
			return &planpb.Node{
				TableDef: &planpb.TableDef{
					Name2ColIndex: map[string]int32{
						catalog.FakePrimaryKeyColName: 0,
						"price":                       1,
					},
					Cols: []*planpb.ColDef{
						{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
						{Name: "price", Typ: columnType},
					},
				},
				FilterList: filterList,
			}
		}

		t.Run("serialized composite falls back", func(t *testing.T) {
			idxPos, filterIdx := builder.getIndexForNonEquiCond(
				[]*planpb.IndexDef{idxDef}, makeNode(filters))

			require.Equal(t, -1, idxPos)
			require.Nil(t, filterIdx)
		})

		directUniqueIdxDef := &planpb.IndexDef{
			Parts:  []string{"price"},
			Unique: true,
		}

		t.Run("direct unique single bound remains eligible", func(t *testing.T) {
			idxPos, filterIdx := builder.getIndexForNonEquiCond(
				[]*planpb.IndexDef{directUniqueIdxDef}, makeNode(filters[:1]))

			require.Equal(t, 0, idxPos)
			require.Equal(t, []int32{0}, filterIdx)
		})

		t.Run("direct unique paired bounds remain eligible", func(t *testing.T) {
			idxPos, filterIdx := builder.getIndexForNonEquiCond(
				[]*planpb.IndexDef{directUniqueIdxDef}, makeNode(filters))

			require.Equal(t, 0, idxPos)
			require.Equal(t, []int32{0, 1}, filterIdx)
		})

		for _, tc := range []struct {
			name    string
			indexes []*planpb.IndexDef
			wantIdx int
		}{
			{
				name:    "direct unique before serialized composite",
				indexes: []*planpb.IndexDef{directUniqueIdxDef, idxDef},
				wantIdx: 0,
			},
			{
				name:    "direct unique after serialized composite",
				indexes: []*planpb.IndexDef{idxDef, directUniqueIdxDef},
				wantIdx: 1,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				idxPos, filterIdx := builder.getIndexForNonEquiCond(tc.indexes, makeNode(filters))

				require.Equal(t, tc.wantIdx, idxPos)
				require.Equal(t, []int32{0, 1}, filterIdx)
			})
		}

		t.Run("direct unique paired fallback checks both bounds", func(t *testing.T) {
			mixedBounds := []*planpb.Expr{
				makeDecimalRangeFilterExpr(t, bindTag, 1, ">=", "10.250000", columnType, higherScaleType),
				filters[1],
			}

			idxPos, filterIdx := builder.getIndexForNonEquiCond(
				[]*planpb.IndexDef{directUniqueIdxDef, idxDef}, makeNode(mixedBounds))

			require.Equal(t, 0, idxPos)
			require.Equal(t, []int32{0, 1}, filterIdx)
		})

		t.Run("direct unique bypasses serialized operator restriction", func(t *testing.T) {
			filter := makeDecimalRangeFilterExpr(t, bindTag, 1, ">", "10.25", columnType, columnType)

			idxPos, filterIdx := builder.getIndexForNonEquiCond(
				[]*planpb.IndexDef{directUniqueIdxDef, idxDef}, makeNode([]*planpb.Expr{filter}))

			require.Equal(t, 0, idxPos)
			require.Equal(t, []int32{0}, filterIdx)
		})
	})
}

func TestGetIndexForNonEquiCond_PrefersFirstPairedRangeByFilterOrder(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	idxPrice := &IndexDef{
		IndexName:      "idx_price",
		Parts:          []string{"price", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "__mo_index_secondary_idx_price",
	}
	idxQuantity := &IndexDef{
		IndexName:      "idx_quantity",
		Parts:          []string{"quantity", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "__mo_index_secondary_idx_quantity",
	}

	node := &planpb.Node{
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name2ColIndex: map[string]int32{
				catalog.FakePrimaryKeyColName: 0,
				"price":                       1,
				"quantity":                    2,
			},
			Cols: []*planpb.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
				{Name: "price", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "quantity", Typ: planpb.Type{Id: int32(types.T_int64)}},
			},
			Pkey:    &planpb.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			Indexes: []*planpb.IndexDef{idxPrice, idxQuantity},
		},
		FilterList: []*planpb.Expr{
			makeRangeFilterExpr(bindTag, 2, ">=", 10),
			makeRangeFilterExpr(bindTag, 2, "<=", 20),
			makeRangeFilterExpr(bindTag, 1, ">=", 100),
			makeRangeFilterExpr(bindTag, 1, "<=", 200),
		},
	}

	idxPos, filterIdx := builder.getIndexForNonEquiCond([]*planpb.IndexDef{idxPrice, idxQuantity}, node)
	require.Equal(t, 1, idxPos)
	require.Equal(t, []int32{0, 1}, filterIdx)
}

func TestGetIndexForNonEquiCond_KeepsEarlierNonPairedFilterPriority(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	idxPrice := &IndexDef{
		IndexName:      "idx_price",
		Parts:          []string{"price", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "__mo_index_secondary_idx_price",
	}
	idxQuantity := &IndexDef{
		IndexName:      "idx_quantity",
		Parts:          []string{"quantity", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "__mo_index_secondary_idx_quantity",
	}

	node := &planpb.Node{
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name2ColIndex: map[string]int32{
				catalog.FakePrimaryKeyColName: 0,
				"price":                       1,
				"quantity":                    2,
			},
			Cols: []*planpb.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
				{Name: "price", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "quantity", Typ: planpb.Type{Id: int32(types.T_int64)}},
			},
			Pkey:    &planpb.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			Indexes: []*planpb.IndexDef{idxPrice, idxQuantity},
		},
		FilterList: []*planpb.Expr{
			makeRangeFilterExpr(bindTag, 1, ">=", 100),
			makeRangeFilterExpr(bindTag, 2, ">=", 10),
			makeRangeFilterExpr(bindTag, 2, "<=", 20),
		},
	}

	idxPos, filterIdx := builder.getIndexForNonEquiCond([]*planpb.IndexDef{idxPrice, idxQuantity}, node)
	require.Equal(t, 0, idxPos)
	require.Equal(t, []int32{0}, filterIdx)
}

func makeEqFilterExpr(colPos int32) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "="},
				Args: []*planpb.Expr{
					{
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: 0,
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_I64Val{I64Val: 1},
							},
						},
					},
				},
			},
		},
	}
}

func makeTestIndexTableDef() *planpb.TableDef {
	return &planpb.TableDef{
		Name: "__mo_index_table",
		Cols: []*planpb.ColDef{
			{
				Name: catalog.IndexTableIndexColName,
				Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			},
			{
				Name: catalog.IndexTablePrimaryColName,
				Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
		},
	}
}

func registerMockIndexTable(t *testing.T, builder *QueryBuilder, indexTableName string) {
	t.Helper()

	key := strings.ToLower(indexTableName)
	objRef := &planpb.ObjectRef{SchemaName: "test", ObjName: indexTableName}
	tableDef := makeTestIndexTableDef()
	tableDef.Name = indexTableName

	switch mockCtx := builder.compCtx.(type) {
	case *MockCompilerContext:
		mockCtx.objects[key] = objRef
		mockCtx.tables[key] = tableDef
	case *fullTextJoinMockCompilerContext:
		mockCtx.objects[key] = objRef
		mockCtx.tables[key] = tableDef
	default:
		t.Fatalf("unexpected compiler context %T", builder.compCtx)
	}
}

func makeStringEqFilterExpr(relPos, colPos int32, val string) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "="},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_Sval{Sval: val},
							},
						},
					},
				},
			},
		},
	}
}

func setIndexFilterArgumentType(expr *planpb.Expr, typ planpb.Type) {
	fn := expr.GetF()
	if fn == nil {
		return
	}
	for _, arg := range fn.Args {
		setExprTypeRecursive(arg, typ)
	}
}

func setIndexRangeArgumentType(expr *planpb.Expr, typ planpb.Type) {
	fn := expr.GetF()
	if fn == nil {
		return
	}
	for i := 0; i < len(fn.Args) && i < 3; i++ {
		setExprTypeRecursive(fn.Args[i], typ)
	}
}

func setExprTypeRecursive(expr *planpb.Expr, typ planpb.Type) {
	if expr == nil {
		return
	}
	expr.Typ = typ
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			setExprTypeRecursive(item, typ)
		}
	}
}

func makeParamEqFilterExpr(relPos, colPos, paramPos int32) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int32)}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "="},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_P{
							P: &planpb.ParamRef{Pos: paramPos},
						},
					},
				},
			},
		},
	}
}

func makeNullEqFilterExpr(relPos, colPos int32) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int32)}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "="},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{Isnull: true},
						},
					},
				},
			},
		},
	}
}

func makeStringInFilterExpr(relPos, colPos int32, vals ...string) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	list := make([]*planpb.Expr, 0, len(vals))
	for _, val := range vals {
		list = append(list, &planpb.Expr{
			Typ: typ,
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{
					Value: &planpb.Literal_Sval{Sval: val},
				},
			},
		})
	}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "in"},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_List{
							List: &planpb.ExprList{List: list},
						},
					},
				},
			},
		},
	}
}

func makeIntInFilterExprWithNull(relPos, colPos int32) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int32)}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "in"},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_List{
							List: &planpb.ExprList{List: []*planpb.Expr{
								{
									Typ: typ,
									Expr: &planpb.Expr_Lit{
										Lit: &planpb.Literal{
											Value: &planpb.Literal_I32Val{I32Val: 1},
										},
									},
								},
								{
									Typ: typ,
									Expr: &planpb.Expr_Lit{
										Lit: &planpb.Literal{Isnull: true},
									},
								},
							}},
						},
					},
				},
			},
		},
	}
}

func makeIntInFilterExpr(relPos, colPos int32, vals ...int32) *planpb.Expr {
	expr := makeIntInFilterExprWithNull(relPos, colPos)
	list := expr.GetF().Args[1].GetList().List[:0]
	for _, val := range vals {
		list = append(list, &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_int32)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: val}}},
		})
	}
	expr.GetF().Args[1].GetList().List = list
	return expr
}

func makeIntBetweenFilterExpr(relPos, colPos, lower, upper int32) *planpb.Expr {
	expr := makeParamBetweenFilterExpr(relPos, colPos, 0, 1)
	expr.GetF().Args[1] = &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: lower}}},
	}
	expr.GetF().Args[2] = &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: upper}}},
	}
	return expr
}

func makeStringBetweenFilterExpr(relPos, colPos int32, lower, upper string) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "between"},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_Sval{Sval: lower},
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_Sval{Sval: upper},
							},
						},
					},
				},
			},
		},
	}
}

func makeStringInRangeFilterExpr(relPos, colPos int32, lower, upper string, flag uint8) *planpb.Expr {
	expr := makeStringBetweenFilterExpr(relPos, colPos, lower, upper)
	expr.GetF().Func.ObjName = "in_range"
	expr.GetF().Args = append(expr.GetF().Args, MakePlan2Uint8ConstExprWithType(flag))
	return expr
}

func makeIntInRangeFilterExpr(relPos, colPos int32, lower, upper int64, flag uint8) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int64)}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "in_range"},
				Args: []*planpb.Expr{
					GetColExpr(typ, relPos, colPos),
					{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: lower}}}},
					{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: upper}}}},
					MakePlan2Uint8ConstExprWithType(flag),
				},
			},
		},
	}
}

func makeStringRangeFilterExpr(relPos, colPos int32, op, value string) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: op},
				Args: []*planpb.Expr{
					GetColExpr(typ, relPos, colPos),
					{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: value}}}},
				},
			},
		},
	}
}

func makeOrFilterExpr(args ...*planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "or"},
				Args: args,
			},
		},
	}
}

func wrappedSerialFuncName(t *testing.T, expr *planpb.Expr) string {
	t.Helper()
	require.NotNil(t, expr)
	fn := expr.GetF()
	require.NotNil(t, fn)
	return fn.Func.ObjName
}

func assertListItemsWrappedBySerialFunc(t *testing.T, expr *planpb.Expr, serialFunc string, expectedLen int) {
	t.Helper()
	require.NotNil(t, expr)
	list := expr.GetList()
	require.NotNil(t, list)
	require.Len(t, list.List, expectedLen)
	for _, item := range list.List {
		assert.Equal(t, serialFunc, wrappedSerialFuncName(t, item))
	}
}

func makeParamInFilterExpr(relPos, colPos int32, n int) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int32)}
	list := make([]*planpb.Expr, 0, n)
	for i := 0; i < n; i++ {
		list = append(list, &planpb.Expr{
			Typ: typ,
			Expr: &planpb.Expr_P{
				P: &planpb.ParamRef{Pos: int32(i)},
			},
		})
	}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "in"},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_List{
							List: &planpb.ExprList{List: list},
						},
					},
				},
			},
		},
	}
}

func makeParamBetweenFilterExpr(relPos, colPos, lowerParamPos, upperParamPos int32) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int32)}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "between"},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_P{
							P: &planpb.ParamRef{Pos: lowerParamPos},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_P{
							P: &planpb.ParamRef{Pos: upperParamPos},
						},
					},
				},
			},
		},
	}
}

func makeParamRangeFilterExpr(relPos, colPos int32, op string, paramPos int32) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int32)}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: op},
				Args: []*planpb.Expr{
					{
						Typ: typ,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{RelPos: relPos, ColPos: colPos},
						},
					},
					{
						Typ: typ,
						Expr: &planpb.Expr_P{
							P: &planpb.ParamRef{Pos: paramPos},
						},
					},
				},
			},
		},
	}
}

func makeRangeFilterExpr(relPos, colPos int32, op string, val int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: op},
				Args: []*planpb.Expr{
					{
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_I64Val{I64Val: val},
							},
						},
					},
				},
			},
		},
	}
}

func makeDecimalRangeFilterExpr(
	t *testing.T,
	relPos, colPos int32,
	op, val string,
	columnType, boundType planpb.Type,
) *planpb.Expr {
	t.Helper()
	decimal, err := types.ParseDecimal64(val, boundType.Width, boundType.Scale)
	require.NoError(t, err)
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: op},
				Args: []*planpb.Expr{
					{
						Typ: columnType,
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{RelPos: relPos, ColPos: colPos},
						},
					},
					{
						Typ: boundType,
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_Decimal64Val{
									Decimal64Val: &planpb.Decimal64{A: int64(decimal)},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeTypedInt64RangeFilterExpr(relPos, colPos int32, op string, val int64, typ planpb.Type) *planpb.Expr {
	expr := makeRangeFilterExpr(relPos, colPos, op, val)
	expr.Typ = planpb.Type{Id: int32(types.T_bool)}
	expr.GetF().Args[0].Typ = typ
	expr.GetF().Args[1].Typ = typ
	return expr
}

func requireSerializedRangeBoundType(t *testing.T, expr *planpb.Expr, want planpb.Type, wantCast bool) {
	t.Helper()
	serial := expr.GetF()
	require.NotNil(t, serial)
	require.Equal(t, "serial", serial.Func.ObjName)
	require.Len(t, serial.Args, 1)
	bound := serial.Args[0]
	require.Equal(t, want.Id, bound.Typ.Id)
	require.Equal(t, want.Width, bound.Typ.Width)
	require.Equal(t, want.Scale, bound.Typ.Scale)
	if wantCast {
		require.NotNil(t, bound.GetF())
		require.Equal(t, "cast", bound.GetF().Func.ObjName)
	} else {
		require.Nil(t, bound.GetF())
	}
}

func makeSpatialConstGeometryExpr() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "st_geomfromtext"},
				Args: []*planpb.Expr{
					{
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_Sval{Sval: "POINT(1 1)"},
							},
						},
					},
				},
			},
		},
	}
}

func makeSpatialColExpr(colPos int32) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: colPos,
			},
		},
	}
}

func makeSpatialDistanceExpr(left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "st_distance"},
				Args: []*planpb.Expr{left, right},
			},
		},
	}
}

func makeComparisonExpr(op string, left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: op},
				Args: []*planpb.Expr{left, right},
			},
		},
	}
}

func makeInt64LiteralExpr(v int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{
				Value: &planpb.Literal_I64Val{I64Val: v},
			},
		},
	}
}

func TestCheckSpatialIndexFilterPredicate(t *testing.T) {
	filter := &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "st_intersects"},
				Args: []*planpb.Expr{
					makeSpatialColExpr(1),
					makeSpatialConstGeometryExpr(),
				},
			},
		},
	}

	col := checkSpatialIndexFilter(filter)
	require.NotNil(t, col)
	require.Equal(t, int32(1), col.ColPos)
}

func TestSpatialIndexOnlyScanInheritsOrderHints(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	tag := builder.genNewBindTag()
	idxDef := &planpb.IndexDef{
		IndexName: "idx_g", IndexAlgo: catalog.MoIndexRTreeAlgo.ToString(),
		IndexTableName: "idx_g_table", Parts: []string{"g"}, TableExist: true,
	}
	tableDef := &planpb.TableDef{
		Name: "spatial_t",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "g", Typ: planpb.Type{Id: int32(types.T_geometry)}},
		},
		Name2ColIndex: map[string]int32{"id": 0, "g": 1},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Indexes:       []*planpb.IndexDef{idxDef},
	}
	filter := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "st_intersects"},
		Args: []*planpb.Expr{makeSpatialColExpr(1), makeSpatialConstGeometryExpr()},
	}}}
	filter.GetF().Args[0].GetCol().RelPos = tag
	scanID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{ObjName: "spatial_t"},
		TableDef: tableDef, BindingTags: []int32{tag}, FilterList: []*planpb.Expr{filter},
	}, ctx)
	require.NoError(t, builder.recordIndexHints(scanID, tableDef, []*tree.IndexHint{{
		HintType: tree.HintIgnore, HintScope: tree.HintForOrderBy, IndexNames: []string{"idx_g"},
	}}))
	registerMockIndexTable(t, builder, idxDef.IndexTableName)

	idxScanID := builder.trySpatialIndexOnlyScan(
		idxDef, builder.qry.Nodes[scanID], map[[2]int32]int{{tag, 0}: 1, {tag, 1}: 1},
		map[[2]int32]*planpb.Expr{}, &Snapshot{},
	)
	require.NotEqual(t, int32(-1), idxScanID)
	idxScan := builder.qry.Nodes[idxScanID]
	require.Equal(t, "idx_g", idxScan.IndexScanInfo.IndexName)
	require.False(t, builder.regularIndexScanAllowedByOrderHints(idxScan))
}

func TestCheckSpatialIndexFilterDistanceComparison(t *testing.T) {
	filter := makeComparisonExpr(
		"<=",
		makeSpatialDistanceExpr(makeSpatialColExpr(2), makeSpatialConstGeometryExpr()),
		makeInt64LiteralExpr(0),
	)

	col := checkSpatialIndexFilter(filter)
	require.NotNil(t, col)
	require.Equal(t, int32(2), col.ColPos)
}

func TestCheckSpatialIndexFilterDistanceComparisonConstOnLeft(t *testing.T) {
	filter := makeComparisonExpr(
		">=",
		makeInt64LiteralExpr(0),
		makeSpatialDistanceExpr(makeSpatialColExpr(3), makeSpatialConstGeometryExpr()),
	)

	col := checkSpatialIndexFilter(filter)
	require.NotNil(t, col)
	require.Equal(t, int32(3), col.ColPos)
}

func TestCheckSpatialIndexFilterDistanceRejectsNonConstGeometryArg(t *testing.T) {
	filter := makeComparisonExpr(
		"<=",
		makeSpatialDistanceExpr(makeSpatialColExpr(1), makeSpatialColExpr(2)),
		makeInt64LiteralExpr(0),
	)

	require.Nil(t, checkSpatialIndexFilter(filter))
}

func TestCheckIndexFilter_RangeOps(t *testing.T) {
	colExpr := makeSpatialColExpr(5)
	constExpr := makeInt64LiteralExpr(10)

	tests := []struct {
		name       string
		op         string
		left       *planpb.Expr
		right      *planpb.Expr
		wantType   int
		wantColPos int32
	}{
		{"col >= const", ">=", colExpr, constExpr, NonEqualIndexCondition, 5},
		{"col <= const", "<=", colExpr, constExpr, NonEqualIndexCondition, 5},
		{"col > const", ">", colExpr, constExpr, NonEqualIndexCondition, 5},
		{"col < const", "<", colExpr, constExpr, NonEqualIndexCondition, 5},
		{"const >= col", ">=", constExpr, colExpr, NonEqualIndexCondition, 5},
		{"const <= col", "<=", constExpr, colExpr, NonEqualIndexCondition, 5},
		{"const > col", ">", constExpr, colExpr, NonEqualIndexCondition, 5},
		{"const < col", "<", constExpr, colExpr, NonEqualIndexCondition, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := makeComparisonExpr(tt.op, tt.left, tt.right)
			fn := filter.GetF()
			gotType, gotCol := checkIndexFilter(fn)
			assert.Equal(t, tt.wantType, gotType)
			require.NotNil(t, gotCol)
			assert.Equal(t, tt.wantColPos, gotCol.ColPos)
		})
	}
}

func TestCanonicalRangeOp(t *testing.T) {
	colExpr := makeSpatialColExpr(1)
	constExpr := makeInt64LiteralExpr(5)

	tests := []struct {
		name  string
		op    string
		left  *planpb.Expr
		right *planpb.Expr
		want  string
	}{
		{"col >= const → >=", ">=", colExpr, constExpr, ">="},
		{"col > const → >", ">", colExpr, constExpr, ">"},
		{"col <= const → <=", "<=", colExpr, constExpr, "<="},
		{"col < const → <", "<", colExpr, constExpr, "<"},
		{"const >= col → <=", ">=", constExpr, colExpr, "<="},
		{"const > col → <", ">", constExpr, colExpr, "<"},
		{"const <= col → >=", "<=", constExpr, colExpr, ">="},
		{"const < col → >", "<", constExpr, colExpr, ">"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := makeComparisonExpr(tt.op, tt.left, tt.right)
			fn := filter.GetF()
			got := canonicalRangeOp(fn)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRangeFilterConstValue(t *testing.T) {
	colExpr := makeSpatialColExpr(1)
	constExpr := makeInt64LiteralExpr(42)

	// col >= const: value is const (right side)
	filter1 := makeComparisonExpr(">=", colExpr, constExpr)
	val1 := rangeFilterConstValue(filter1.GetF())
	require.NotNil(t, val1)
	assert.Equal(t, int64(42), val1.GetLit().GetI64Val())

	// const < col: value is const (left side)
	filter2 := makeComparisonExpr("<", constExpr, colExpr)
	val2 := rangeFilterConstValue(filter2.GetF())
	require.NotNil(t, val2)
	assert.Equal(t, int64(42), val2.GetLit().GetI64Val())
}

func TestIsRangeOp(t *testing.T) {
	tests := []struct {
		op       string
		expected bool
	}{
		{">=", true},
		{">", true},
		{"<=", true},
		{"<", true},
		{"in_range", true},
		{"=", false},
		{"in", false},
		{"between", false},
		{"or", false},
		{"prefix_in_range", false},
	}
	for _, tt := range tests {
		fn := &planpb.Function{Func: &planpb.ObjectRef{ObjName: tt.op}}
		assert.Equal(t, tt.expected, isRangeOp(fn), "isRangeOp(%q)", tt.op)
	}
}
