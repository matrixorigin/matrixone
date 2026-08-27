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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestValidateLifecycleRestoreTableAccess(t *testing.T) {
	ctx := context.Background()
	require.Error(t, ValidateLifecycleRestoreTableAccess(
		ctx,
		true,
		catalog.LifecycleRestoreTableNamePrefix+
			"0123456789abcdef0123456789abcdef",
	))
	require.NoError(t, ValidateLifecycleRestoreTableAccess(
		ctx,
		false,
		catalog.LifecycleRestoreTableNamePrefix+
			"0123456789abcdef0123456789abcdef",
	))
	require.NoError(t, ValidateLifecycleRestoreTableAccess(
		ctx,
		true,
		"events_history",
	))
}

func TestBuildDDLReservesLifecycleRestoreStagingNamesForFrontend(t *testing.T) {
	frontendContext := NewMockCompilerContext(false)
	proc := testutil.NewProc(nil)
	proc.Base.IsFrontend = true
	frontendContext.GetProcessFunc = func() *process.Process { return proc }

	reservedName := catalog.LifecycleRestoreTableNamePrefix +
		"0123456789abcdef0123456789abcdef"
	for _, sql := range []string{
		"create table tpch." + reservedName + "(id bigint)",
		"create view tpch." + reservedName + " as select 1",
		"create sequence tpch." + reservedName,
		"rename table tpch.nation to tpch." + reservedName,
		"alter table tpch.nation rename to tpch." + reservedName,
	} {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		_, err = BuildPlan(frontendContext, stmt, false)
		stmt.Free()
		require.ErrorContains(t, err, "Lifecycle Restore staging")
	}

	internalContext := NewMockCompilerContext(false)
	stmt, err := parsers.ParseOne(
		context.Background(),
		dialect.MYSQL,
		"create table tpch."+reservedName+"(id bigint)",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPlan(internalContext, stmt, false)
	require.NoError(t, err)
}
