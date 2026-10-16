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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func buildLoadNullIfTestPlan(t *testing.T, expression string, legacy bool) (*planpb.Plan, error) {
	t.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	sql := strings.Replace(issue29849LoadSQL, "NULLIF(@v, 'NULL')", expression, 1)
	statement, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer statement.Free()
	load := statement.(*tree.Load)
	original := load.Param.Tail.Assignments[0].Expr
	before := tree.String(original, dialect.MYSQL)
	var result *planpb.Plan
	if legacy {
		nation := ctx.tables["nation"]
		nation.Name2ColIndex = make(map[string]int32, len(nation.Cols))
		for i, column := range nation.Cols {
			nation.Name2ColIndex[column.Name] = int32(i)
		}
		result, err = buildLoad(load, ctx, false)
	} else {
		result, err = BuildPlan(ctx, statement, false)
	}
	// Replacing @v with the scan column must not modify the caller's AST.
	require.Equal(t, before, tree.String(original, dialect.MYSQL))
	return result, err
}

func TestLoadNullIfCollatedUserVariable(t *testing.T) {
	for _, tc := range []struct {
		name string
		expr string
	}{
		{"plain", "NULLIF(@v, 'NULL')"},
		{"collated", "NULLIF(@v, 'NULL') COLLATE utf8mb4_bin"},
		{"nested wrappers", "((NULLIF(@v, 'NULL') COLLATE utf8mb4_bin)) COLLATE utf8mb4_bin"},
	} {
		for _, legacy := range []bool{false, true} {
			mode := "modern"
			if legacy {
				mode = "legacy"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				result, err := buildLoadNullIfTestPlan(t, tc.expr, legacy)
				require.NoError(t, err)
				var expectedCollation uint32
				if tc.name != "plain" {
					expectedCollation = uint32(types.CharsetUTF8MB4Bin)
				}
				assertLoadUserVariablePlan(t, result, expectedCollation)
			})
		}
	}
}

func TestLoadNullIfCollatedPayloadPreservesRawDomain(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "modern", true: "legacy"}[legacy], func(t *testing.T) {
			built, err := buildLoadNullIfTestPlan(t, "NULLIF(@v, 'NULL') COLLATE utf8mb4_bin", legacy)
			require.NoError(t, err)
			expression := DeepCopyExpr(assertLoadUserVariablePlan(t, built, uint32(types.CharsetUTF8MB4Bin)))
			base := expression
			for base.GetF().GetFunc().GetObjName() == "cast" {
				base = base.GetF().GetArgs()[0]
			}
			replaceColRefTag(expression, base.GetF().GetArgs()[2].GetCol().RelPos, 0)
			proc := newPlanTestProcess(t)
			input := batch.NewWithSize(4)
			defer input.Clean(proc.Mp())
			input.Vecs[3] = vector.NewVec(types.New(types.T_text, types.MaxLongTextLen, 0))
			values := []string{"keep", strings.Repeat("x", types.MaxVarcharLen+1), "NULL", ""}
			for i, value := range values {
				require.NoError(t, vector.AppendBytes(input.Vecs[3], []byte(value), i == 3, proc.Mp()))
			}
			input.SetRowCount(len(values))
			executor, err := colexec.NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			for i := range 2 {
				require.False(t, result.GetNulls().Contains(uint64(i)))
				require.Equal(t, values[i], string(result.GetBytesAt(i)))
			}
			require.True(t, result.GetNulls().Contains(2))
			require.True(t, result.GetNulls().Contains(3))
		})
	}
}

func TestExternalNullIfCollatedColumnRequiresProjection(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	statement, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"create external table collated_nullif(s varchar(32)) infile{'filepath'='ignored.csv'} fields terminated by ',' set s=nullif(s,'sentinel') collate utf8mb4_bin", 1)
	require.NoError(t, err)
	defer statement.Free()
	_, err = BuildPlan(ctx, statement, false)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
}

func TestLoadNullIfCollationErrors(t *testing.T) {
	for _, tc := range []struct {
		expr string
		code uint16
	}{
		{"NULLIF(@v, 'NULL') COLLATE no_such_load_collation", moerr.ErrUnknownCollation},
		{"NULLIF(@v, 'NULL') COLLATE utf8_unicode_ci", moerr.ErrCollationCharsetMismatch},
		{"NULLIF(n_comment, 'NULL') COLLATE utf8mb4_bin", moerr.ErrNotSupported},
		{"NULLIF(@missing, 'NULL') COLLATE utf8mb4_bin", moerr.ErrInvalidInput},
		{"NULLIF(@@v, 'NULL') COLLATE utf8mb4_bin", moerr.ErrInvalidInput},
		{"NULLIF(@v, concat('N', 'ULL')) COLLATE utf8mb4_bin", moerr.ErrInvalidInput},
	} {
		for _, legacy := range []bool{false, true} {
			t.Run(tc.expr+"/legacy="+map[bool]string{false: "false", true: "true"}[legacy], func(t *testing.T) {
				_, err := buildLoadNullIfTestPlan(t, tc.expr, legacy)
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, tc.code), "%v", err)
			})
		}
	}
}
