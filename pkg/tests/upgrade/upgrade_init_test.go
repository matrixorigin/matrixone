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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v1_2_0"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestUpgradeFrameworkInit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	runUpgradeTest(
		t,
		func(opts service.Options) service.Options {
			return opts.WithCNOptionFunc(func(i int) []cnservice.Option {
				return []cnservice.Option{
					cnservice.WithBootstrapOptions(
						bootstrap.WithCheckUpgradeDuration(time.Millisecond*100),
						bootstrap.WithCheckUpgradeTenantDuration(time.Millisecond*100),
						bootstrap.WithCheckUpgradeTenantWorkers(1),
						bootstrap.WithUpgradeTenantBatch(1),
						bootstrap.WithUpgradeHandles([]bootstrap.VersionHandle{
							v1_2_0.Handler,
						})),
				}
			})
		},
		func(c service.Cluster) {
			waitVersionReady(t, "1.2.0", c)
			checkVersionUpgrades(t, "1.2.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 0, len(upgrades))
			})
		})
}

func TestUpgradeFrameworkInitWithHighVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	h := newTestVersionHandler("1.3.0", "1.2.0", versions.Yes, versions.No)

	runUpgradeTest(
		t,
		func(opts service.Options) service.Options {
			return opts.WithCNOptionFunc(func(i int) []cnservice.Option {
				return []cnservice.Option{
					cnservice.WithBootstrapOptions(
						bootstrap.WithCheckUpgradeDuration(time.Millisecond*100),
						bootstrap.WithCheckUpgradeTenantDuration(time.Millisecond*100),
						bootstrap.WithCheckUpgradeTenantWorkers(1),
						bootstrap.WithUpgradeTenantBatch(1),
						bootstrap.WithUpgradeHandles([]bootstrap.VersionHandle{
							v1_2_0.Handler,
							h,
						})),
				}
			})
		},
		func(c service.Cluster) {
			waitVersionReady(t, "1.3.0", c)
			checkVersionUpgrades(t, "1.3.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 0, len(upgrades))
			})
		})
}

func checkVersionUpgrades(
	t *testing.T,
	version string,
	c service.Cluster,
	ck func([]versions.VersionUpgrade)) {
	svc, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	exec := svc.GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	now, _ := c.Clock().Now()
	opts := executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithMinCommittedTS(now)
	err = exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			upgrades, err := versions.GetUpgradeVersions(version, 0, txn, false, false)
			require.NoError(t, err)
			ck(upgrades)
			return err
		},
		opts)
	require.NoError(t, err)
}

func waitVersionReady(
	t *testing.T,
	version string,
	c service.Cluster) {
	svc, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	exec := svc.GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	for {
		ready := false
		opts := executor.Options{}.WithDatabase(catalog.MO_CATALOG)
		err := exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				v, _, err := versions.GetVersionState(version, 0, txn, false)
				if err != nil {
					return err
				}
				ready = v == versions.StateReady
				return nil
			},
			opts)
		require.NoError(t, err)
		if ready {
			return
		}
		time.Sleep(time.Second)
	}
}

var (
	accountIndex atomic.Uint64
)

func createTenants(
	t *testing.T,
	c service.Cluster,
	n int,
	version string) []int32 {
	svc, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	exec := svc.GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	var ids []int32
	opts := executor.Options{}.WithWaitCommittedLogApplied().WithDatabase(catalog.MO_CATALOG)
	err = exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			for i := 0; i < n; i++ {
				sql := fmt.Sprintf(`insert into mo_catalog.mo_account(
					account_name,
					status,
					created_time,
					comments,
					create_version) values ('test_%d','open',current_timestamp(),' ','%s');`,
					accountIndex.Add(1),
					version)
				res, err := txn.Exec(sql, executor.StatementOption{})
				if err != nil {
					return err
				}
				ids = append(ids, int32(res.LastInsertID))
				res.Close()
			}
			return nil
		},
		opts)
	require.NoError(t, err)
	return ids
}

func checkTenantVersion(
	t *testing.T,
	c service.Cluster,
	version string,
	ids ...int32) {
	svc, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	exec := svc.GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	opts := executor.Options{}.WithWaitCommittedLogApplied().WithDatabase(catalog.MO_CATALOG)

	sql := "select account_id, create_version from mo_account"
	if len(ids) > 0 {
		sql += " where account_id in ("
		for _, id := range ids {
			sql += fmt.Sprintf("%d,", id)
		}
		sql = sql[:len(sql)-1] + ")"
	}

	res, err := exec.Exec(ctx, sql, opts)
	require.NoError(t, err)
	defer res.Close()

	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			require.Equal(t, version, cols[1].GetStringAt(i))
		}
		return true
	})
}

func runUpgradeTest(
	t *testing.T,
	adjustOptions func(service.Options) service.Options,
	fn func(c service.Cluster),
) {
	ctx := context.Background()
	opts := service.DefaultOptions()
	if adjustOptions != nil {
		opts = adjustOptions(opts)
	}

	c, err := service.NewCluster(
		ctx,
		t,
		opts)
	require.NoError(t, err)
	// close the cluster
	defer func(c service.Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	fn(c)
}

func newTestVersionHandler(
	version, minVersion string,
	upgradeCluster, upgradeTenant int32) *testVersionHandle {
	return &testVersionHandle{
		metadata: versions.Version{
			Version:           version,
			MinUpgradeVersion: minVersion,
			UpgradeCluster:    upgradeCluster,
			UpgradeTenant:     upgradeTenant,
		},
	}
}

type testVersionHandle struct {
	metadata                 versions.Version
	callHandleClusterUpgrade atomic.Uint64
	callHandleTenantUpgrade  atomic.Uint64
}

func (h *testVersionHandle) Metadata() versions.Version {
	return h.metadata
}
func (h *testVersionHandle) Prepare(ctx context.Context, txn executor.TxnExecutor, final bool) error {
	return nil
}
func (h *testVersionHandle) HandleClusterUpgrade(ctx context.Context, txn executor.TxnExecutor) error {
	h.callHandleClusterUpgrade.Add(1)
	return nil
}
func (h *testVersionHandle) HandleTenantUpgrade(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	h.callHandleTenantUpgrade.Add(1)
	return nil
}
