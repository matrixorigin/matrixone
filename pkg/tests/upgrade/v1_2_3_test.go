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

package upgrade

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v1_2_3"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func Test_UpgradeEntry(t *testing.T) {
	c, err := embed.NewCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	require.NoError(t, c.Start())

	svc, err := c.GetCNService(0)
	require.NoError(t, err)

	exec := testutils.GetSQLExecutor(svc)
	require.NotNil(t, exec)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	err = exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		err = v1_2_3.Handler.HandleClusterUpgrade(ctx, txn)
		require.NoError(t, err)

		err = v1_2_3.Handler.HandleTenantUpgrade(ctx, 0, txn)
		require.NoError(t, err)

		return nil
	}, executor.Options{}.WithWaitCommittedLogApplied())
	require.NoError(t, err)
}
