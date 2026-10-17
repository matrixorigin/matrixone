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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestLoadSetRejectsUnsupportedExpressions(t *testing.T) {
	for _, tc := range []struct {
		name, assignment string
	}{
		{"constant", "n_comment='CONSTVAL'"},
		{"null", "n_comment=NULL"},
		{"column", "n_comment=n_name"},
		{"variable", "n_comment=@v"},
		{"arithmetic", "n_nationkey=n_nationkey+1"},
		{"function", "n_comment=upper(@v)"},
		{"two argument function", "n_comment=concat(n_name,'!')"},
		{"nullif arity", "n_comment=nullif(n_comment)"},
		{"valid then unsupported", "n_name=nullif(n_name,'NULL'),n_comment='CONSTVAL'"},
		{"unsupported then valid", "n_comment='CONSTVAL',n_name=nullif(n_name,'NULL')"},
		{"same target", "n_comment=nullif(n_comment,'NULL'),n_comment='CONSTVAL'"},
		{"collated constant", "n_comment='CONSTVAL' COLLATE utf8mb4_bin"},
		{"collated unsupported function", "n_comment=upper(@v) COLLATE utf8mb4_bin"},
		{"collated then unsupported", "n_comment=nullif(@v,'NULL') COLLATE utf8mb4_bin,n_name='CONSTVAL'"},
		{"unsupported then collated", "n_name='CONSTVAL',n_comment=nullif(@v,'NULL') COLLATE utf8mb4_bin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testLoadSetRejectedWithoutMutation(t, tc.assignment, moerr.ErrNotSupported)
		})
	}
}

func TestLoadSetMalformedNullIfDoesNotConsumeEarlierAssignments(t *testing.T) {
	for _, tc := range []struct {
		name, assignment string
	}{
		{"first argument", "n_comment=nullif(upper(n_comment),'NULL')"},
		{"second argument", "n_comment=nullif(n_comment,upper('NULL'))"},
		{"target mismatch", "n_comment=nullif(n_name,'NULL')"},
		{"system variable", "n_comment=nullif(@@sql_mode,'NULL')"},
		{"variable second argument", "n_comment=nullif(@v,upper('NULL'))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testLoadSetRejectedWithoutMutation(t, "n_name=nullif(n_name,'NULL'),"+tc.assignment, moerr.ErrInvalidInput)
		})
	}
}

func TestLoadSetCollatedNullIfValidation(t *testing.T) {
	for _, tc := range []struct {
		expression string
		wantError  bool
	}{
		{expression: "NULLIF(@v,'NULL') COLLATE utf8mb4_bin"},
		{expression: "(NULLIF(@v,'NULL')) COLLATE utf8mb4_unicode_ci"},
		{expression: "((NULLIF(@v,'NULL') COLLATE utf8mb4_bin)) COLLATE utf8mb4_bin"},
		{expression: "NULLIF(n_name,'NULL') COLLATE utf8mb4_bin", wantError: true},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			statement, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
				"LOAD DATA INLINE FORMAT='csv', DATA='x,y' INTO TABLE nation SET n_name=nullif(n_name,'NULL'),n_name="+tc.expression, 1)
			require.NoError(t, err)
			defer statement.Free()
			load := statement.(*tree.Load)
			original := tree.String(load, dialect.MYSQL)
			for range 2 {
				err := validateLoadSetAssignments(load.Param, ctx)
				if tc.wantError {
					require.ErrorContains(t, err, "COLLATE on column NULLIF")
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, original, tree.String(load, dialect.MYSQL))
				require.Nil(t, load.Param.NullMap, "validation must not consume earlier assignments")
			}
		})
	}
}

func TestLoadSetSupportedNullIfRebind(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	statement, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"LOAD DATA INLINE FORMAT='csv', DATA='x,y' INTO TABLE nation SET n_name=NuLlIf(n_name,'NULL'),n_name=nullif(n_name,'')", 1)
	require.NoError(t, err)
	defer statement.Free()
	param := statement.(*tree.Load).Param
	for range 2 {
		require.NoError(t, validateLoadSetAssignments(param, ctx))
		require.NoError(t, InitNullMap(param, ctx))
		require.Equal(t, map[string][]string{"n_name": {"NULL", ""}}, param.NullMap)
	}
	// nil Expr is an internal consumed marker. It must not hide a later raw
	// unsupported expression, even though InitNullMap accepts the marker.
	param.Tail.Assignments = append(param.Tail.Assignments, &tree.UpdateExpr{
		Expr: tree.NewNumVal("x", "x", false, tree.P_char),
	})
	require.True(t, moerr.IsMoErrCode(validateLoadSetAssignments(param, ctx), moerr.ErrNotSupported))
}

func testLoadSetRejectedWithoutMutation(t *testing.T, assignments string, code uint16) {
	t.Helper()
	for _, legacy := range []bool{false, true} {
		name := "modern"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			statement, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
				"LOAD DATA INLINE FORMAT='csv', DATA='1,2,alpha,NULL' INTO TABLE nation FIELDS TERMINATED BY ',' (n_nationkey,n_regionkey,n_name,@v) SET "+assignments, 1)
			require.NoError(t, err)
			defer statement.Free()
			load := statement.(*tree.Load)
			nation := ctx.tables["nation"]
			nation.Name2ColIndex = make(map[string]int32, len(nation.Cols))
			for i, column := range nation.Cols {
				nation.Name2ColIndex[column.Name] = int32(i)
			}
			original := tree.String(load, dialect.MYSQL)
			for pass := range 2 {
				var built *Plan
				if legacy {
					built, err = buildLoad(load, ctx, false)
				} else {
					built, err = BuildPlan(ctx, load, false)
				}
				require.Error(t, err, "pass %d", pass)
				require.Nil(t, built)
				require.True(t, moerr.IsMoErrCode(err, code), "%v", err)
				if code == moerr.ErrNotSupported {
					require.Contains(t, err.Error(), "LOAD DATA SET only supports NULLIF")
				}
				require.Equal(t, original, tree.String(load, dialect.MYSQL), "rejected SET must remain intact")
				require.Nil(t, load.Param.NullMap)
			}
		})
	}
}
