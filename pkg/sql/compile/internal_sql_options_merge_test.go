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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestInternalSQLOptionOwnership(t *testing.T) {
	proc := testutil.NewProcess(t)
	var received []executor.Options
	spy := &dropDDLExecutor{exec: func(_ context.Context, _ string, opts executor.Options) (executor.Result, error) {
		received = append(received, opts)
		return executor.Result{}, nil
	}}
	installDropDDLExecutor(t, proc, spy)
	c := &Compile{proc: proc}
	adjusted := 0
	adjust := func(*api.SchemaExtra) error { adjusted++; return nil }
	spec := maintenanceTestSpec()
	validator := maintenanceTestValidator{}

	_, err := c.runSqlWithResultAndOptions("select 1", NoAccountId, executor.StatementOption{})
	require.NoError(t, err)
	_, err = c.runSqlWithResultAndOptionsAndExtra("create table t(a int)", NoAccountId, executor.StatementOption{}, adjust)
	require.NoError(t, err)
	_, err = c.runSqlWithResultAndMaintenance("select maintenance", NoAccountId, executor.StatementOption{}, spec, validator)
	require.NoError(t, err)
	require.Len(t, received, 3)

	for i, opts := range received {
		require.NoError(t, opts.AdjustTableExtraFunc()(&api.SchemaExtra{}))
		if i == 1 {
			require.Equal(t, 1, adjusted, "the CREATE owns the extra adjuster")
		} else {
			require.Equal(t, min(i, 1), adjusted, "unrelated internal SQL must not inherit the adjuster")
		}
		if i == 2 {
			require.Equal(t, spec, opts.IndexMaintenanceSpec())
			require.NotNil(t, opts.IndexMaintenanceValidator())
		} else {
			require.Nil(t, opts.IndexMaintenanceSpec())
			require.Nil(t, opts.IndexMaintenanceValidator())
		}
	}
}
