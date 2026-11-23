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
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestIndexMaintenanceViewBindingDoesNotEvaluate(t *testing.T) {
	previous := fault.Status()
	t.Cleanup(func() {
		if previous {
			fault.Enable()
		} else {
			fault.Disable()
		}
	})
	fault.Disable()
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctx := defines.WithIndexMaintenanceViewBinding(context.Background())
	compiler := NewMockCompilerContext(true, proc)
	compiler.SetContext(ctx)
	for _, sql := range []string{
		"select n_name from tpch.nation where cast(enable_fault_injection() as signed) = 0",
		"select o.o_orderkey, (with x as (select count(*) as c from tpch.orders i where i.o_orderkey = o.o_orderkey) select c + cast(enable_fault_injection() as signed) from x) as n from tpch.orders o",
		"select cast(if(enable_fault_injection(), cast('2024-01-01 00:00:00' as datetime), cast('2025-01-01 00:00:00' as datetime)) as signed) from tpch.nation",
		"select unzip_file(concat('/unused/',cast(enable_fault_injection() as char))) from tpch.nation",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.NoError(t, BindIndexMaintenanceView(compiler, stmt.(*tree.Select)))
			require.False(t, fault.Status(), "binding a consumed view must not evaluate its expressions")
		})
	}
	expr, err := BindFuncExprImplByPlanExpr(ctx, "enable_fault_injection", nil)
	require.NoError(t, err)
	builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
	_, _ = literalUint64ForStats(expr, builder)
	require.False(t, fault.Status(), "bind-time statistics must not evaluate a LIMIT expression")
	compiler.SetContext(defines.WithoutIndexMaintenanceViewBinding(ctx))
	_, _ = literalUint64ForStats(expr, builder)
	require.True(t, fault.Status(), "ordinary statistics retain their existing folding behavior")
	require.False(t, defines.IsIndexMaintenanceViewBinding(defines.WithoutIndexMaintenanceViewBinding(ctx)))
}

func TestIndexMaintenanceViewBindingSkipsCatalogExpressionMigration(t *testing.T) {
	compiler := NewMockCompilerContext(false, nil)
	compiler.SetContext(defines.WithIndexMaintenanceViewBinding(t.Context()))
	source := legacyFloatTable(t, "generated")
	before := proto.Clone(source)
	calls := 0
	compiler.GetProcessFunc = func() *process.Process { calls++; return nil }
	require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, source))
	require.Zero(t, calls)
	require.True(t, proto.Equal(before, source))
	require.NoError(t, MigrateLegacyCatalogTableDef(compiler.GetContext(), nil, source))
	require.True(t, proto.Equal(before, source))
	compiler.SetContext(t.Context())
	require.ErrorContains(t, MigrateLegacyFloatStringTableDef(compiler, source), "requires a process")
	require.Positive(t, calls)
}
