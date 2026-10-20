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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func nativePADSetOperationMock(t *testing.T, charset uint32) *MockOptimizer {
	t.Helper()
	ctx, _ := nativePADDDLContext(t)
	// 使用真正的IMPLICIT列域；派生显式COLLATE会以更强rank掩盖此回归。
	table := DeepCopyTableDef(ctx.tables["nation"], true)
	column := FindColumn(table.Cols, "n_name")
	require.NotNil(t, column)
	column.Typ.Charset, column.Typ.CollationVersion = charset, 1
	ctx.tables["nation"] = table
	return &MockOptimizer{ctxt: *ctx}
}

func TestNativePADSetOperationPureNullProvenance(t *testing.T) {
	for _, domain := range []struct {
		name    string
		charset uint32
	}{
		{"utf8mb4_general_ci", uint32(types.CharsetUTF8)},
		{"utf8mb4_bin", uint32(types.CharsetUTF8MB4Bin)},
	} {
		for _, tc := range []struct {
			name, prefix, nullBranch string
		}{
			{"literal", "", "select null as x"},
			{"derived", "", "select x from (select null as x) n"},
			{"cte", "with n as (select null as x) ", "select x from n"},
		} {
			for _, reverse := range []bool{false, true} {
				for _, prepared := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/reverse=%v/prepare=%v", domain.name, tc.name, reverse, prepared), func(t *testing.T) {
						mock := nativePADSetOperationMock(t, domain.charset)
						left, right := "select n_name as x from nation", tc.nullBranch
						if reverse {
							left, right = right, left
						}
						sql := tc.prefix + "select min(x), max(x) from (" + left + " union all " + right + ") u"
						if prepared {
							sql = "prepare native_union from '" + strings.ReplaceAll(sql, "'", "''") + "'"
						}
						p, err := runOneStmt(mock, t, sql)
						require.NoError(t, err)
						if prepared {
							p = p.GetDcl().GetPrepare().Plan
						}
						columns := GetResultColumnsFromPlan(p)
						require.Len(t, columns, 2)
						for _, col := range columns {
							require.Equal(t, domain.charset, col.Typ.Charset)
							require.Equal(t, uint32(1), col.Typ.CollationVersion)
						}
					})
				}
			}
		}
	}
	for _, branch := range []string{
		"select x from (select cast(null as char(8)) as x) n",
		"select n_comment as x from nation",
	} {
		t.Run("declared-domain/"+branch, func(t *testing.T) {
			mock := nativePADSetOperationMock(t, uint32(types.CharsetUTF8))
			_, err := runOneStmt(mock, t, "select n_name as x from nation union all "+branch)
			require.ErrorContains(t, err, "Illegal mix of collations")
		})
	}
}

func TestNativePADAllPureNullSetOperationStaysLegacy(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	p, err := runOneStmt(mock, t, "select x from (select null as x) n union all select x from (select null as x) m")
	require.NoError(t, err)
	columns := GetResultColumnsFromPlan(p)
	require.Len(t, columns, 1)
	require.Zero(t, columns[0].Typ.CollationVersion)
}

func TestNativePADPreparedSetUsesExecutableAdmission(t *testing.T) {
	for _, expression := range []string{
		"'a' collate utf8mb4_bin",
		"(select 'a') collate utf8mb4_bin",
		"concat((select 'left') collate utf8mb4_bin, (select 'right') collate utf8mb4_bin)",
		"(select n_name from nation where n_nationkey = 1) collate utf8mb4_bin",
		"? collate utf8mb4_general_ci",
	} {
		t.Run(expression, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			sql := "set @native_value = " + expression
			p, err := runOneStmt(mock, t, "prepare native_set from '"+strings.ReplaceAll(sql, "'", "''")+"'")
			require.NoError(t, err)
			prepared := p.GetDcl().GetPrepare()
			require.NotNil(t, prepared)
			require.NotNil(t, prepared.Plan.GetDcl().GetSetVariables())
			features, err := pb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.True(t, features.NativePADCollation)
			// 执行owner选择不能全局放松旧schema/codec门。
			require.Error(t, pb.RequireLegacyCollations(prepared.Plan))
			if strings.Contains(expression, "?") {
				require.Len(t, prepared.ParamTypes, 1)
			} else {
				direct, err := runOneStmt(mock, t, sql)
				require.NoError(t, err)
				require.NotNil(t, direct.GetDcl().GetSetVariables())
			}
		})
	}
	t.Run("prepared-persisted-computation-still-rejected", func(t *testing.T) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		_, err := runOneStmt(mock, t, "prepare native_ddl from 'create table denied (v int default (length(''a'' collate utf8mb4_general_ci)))'")
		require.ErrorContains(t, err, "native PAD collation persistence is disabled")
	})
}
