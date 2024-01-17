package upgrade

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v1_2_0"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestUpgradeFrameworkInit(t *testing.T) {
	runUpgradeTest(
		t,
		func(opts service.Options) service.Options {
			return opts.WithCNOptionFunc(func(i int) []cnservice.Option {
				return []cnservice.Option{
					cnservice.WithBootstrapOptions(
						bootstrap.WithUpgradeHandles([]bootstrap.VersionHandle{
							v1_2_0.Handler,
						})),
				}
			})
		},
		func(c service.Cluster) {
			waitVersionReady(t, "1.2.0", c)
			checkVersionUpgrade(t, "1.2.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 0, len(upgrades))
			})
		})
}

func TestUpgradeFrameworkInitWithHighVersion(t *testing.T) {
	h := newTestVersionHandler("1.3.0", "1.2.0", versions.Yes, versions.No)

	runUpgradeTest(
		t,
		func(opts service.Options) service.Options {
			return opts.WithCNOptionFunc(func(i int) []cnservice.Option {
				return []cnservice.Option{
					cnservice.WithBootstrapOptions(
						bootstrap.WithUpgradeHandles([]bootstrap.VersionHandle{
							v1_2_0.Handler,
							h,
						})),
				}
			})
		},
		func(c service.Cluster) {
			waitVersionReady(t, "1.3.0", c)
			checkVersionUpgrade(t, "1.3.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 0, len(upgrades))
			})
		})
}

func checkVersionUpgrade(
	t *testing.T,
	version string,
	c service.Cluster,
	ck func([]versions.VersionUpgrade)) {
	svc, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	exec := svc.GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	now, _ := c.Clock().Now()
	opts := executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithMinCommittedTS(now)
	err = exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			upgrades, err := versions.GetUpgradeVersions(version, txn, false, false)
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
				v, err := versions.GetLatestVersion(txn)
				if err != nil {
					return err
				}
				ready = v.IsReady()
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
	upgradeCluster, upgradeTenant int32) bootstrap.VersionHandle {
	return &testVersionHandle{
		metadata: versions.Version{
			Version:           version,
			MinUpgradeVersion: version,
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
