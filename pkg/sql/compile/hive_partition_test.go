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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunFilePathFilters_LeftoverContainsUnconsumed locks the compile-layer
// contract that motivates the fpFilters → rowFilters append in
// getHivePartitionFileList. If runFilePathFilters is modified (or deleted) to
// no longer surface FilterFileList's unconsumed predicates as its leftover
// return value, queries like
//
//	WHERE __mo_filepath LIKE '%x%' OR false
//
// will silently skip their predicate and return wrong rows. This test fires
// immediately in that scenario.
func TestRunFilePathFilters_LeftoverContainsUnconsumed(t *testing.T) {
	proc := testutil.NewProc(t)

	td := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "year"},
			{Name: catalog.ExternalFilePath},
		},
	}
	orExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.OR) << 32, ObjName: "or"},
			Args: []*plan.Expr{
				{
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{Obj: function.EqualFunctionEncodedID},
						Args: []*plan.Expr{
							{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: catalog.ExternalFilePath}}},
							{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "x"}}}},
						},
					}},
				},
				{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}},
			},
		}},
	}

	fileList := []string{"/warehouse/data/year=2024/f.parquet"}
	fileSize := []int64{100}

	outFileList, outFileSize, leftover, err := runFilePathFilters(
		proc.Ctx, proc, td, []*plan.Expr{orExpr}, fileList, fileSize)
	require.NoError(t, err)

	assert.Equal(t, fileList, outFileList,
		"FilterFileList short-circuits when judgeContainColname rejects all fpFilters")
	assert.Equal(t, fileSize, outFileSize)
	require.Equal(t, 1, len(leftover),
		"unconsumed OR(filepath, literal) must be returned in leftover so the caller can "+
			"append it to rowFilters; losing it silently drops the predicate at runtime")
	assert.Same(t, orExpr, leftover[0])
}

// TestToLowerSet covers the tiny helper feeding partColSet.
func TestToLowerSet(t *testing.T) {
	got := toLowerSet([]string{"Year", "MONTH", "day"})
	assert.True(t, got["year"])
	assert.True(t, got["month"])
	assert.True(t, got["day"])
	assert.False(t, got["Year"])
	assert.Equal(t, 3, len(got))

	// Empty input.
	got = toLowerSet(nil)
	assert.Equal(t, 0, len(got))
}

// TestRunFilePathFilters_AllConsumed exercises the consumed-path of
// runFilePathFilters where FilterFileList keeps the filter and leftover is
// empty — distinct from the unconsumed regression above.
func TestRunFilePathFilters_NoFilters(t *testing.T) {
	proc := testutil.NewProc(t)
	td := &plan.TableDef{Cols: []*plan.ColDef{{Name: catalog.ExternalFilePath}}}
	// Empty fpFilters → FilterFileList short-circuits, returns fileList unchanged.
	fileList := []string{"/a.parquet"}
	fileSize := []int64{10}
	out, outSz, leftover, err := runFilePathFilters(proc.Ctx, proc, td, nil, fileList, fileSize)
	require.NoError(t, err)
	assert.Equal(t, fileList, out)
	assert.Equal(t, fileSize, outSz)
	assert.Empty(t, leftover)
}
