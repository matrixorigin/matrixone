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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v1_2_0"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/stretchr/testify/require"
)

func TestUpgrade(t *testing.T) {
	h := newTestVersionHandler("1.3.0", "1.2.0", versions.Yes, versions.Yes)
	runUpgradeTest(
		t,
		func(opts service.Options) service.Options {
			return opts.WithCNOptionFunc(func(i int) []cnservice.Option {
				if i == 0 {
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
				}
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
			waitVersionReady(t, "1.2.0", c)
			checkVersionUpgrades(t, "1.2.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 0, len(upgrades))
			})
			createTenants(t, c, 10, "1.2.0")

			require.NoError(t, c.StartCNServices(2))
			waitVersionReady(t, "1.3.0", c)
			checkVersionUpgrades(t, "1.3.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 1, len(upgrades))
				checkVersionUpgrade(t, upgrades[0], "1.2.0", "1.3.0", "1.3.0", 11)
			})
			checkTenantVersion(t, c, "1.3.0")
		})
}

func TestUpgradeCrossVersions(t *testing.T) {
	h1 := newTestVersionHandler("1.3.0", "1.2.0", versions.Yes, versions.No)
	h2 := newTestVersionHandler("1.4.0", "1.3.0", versions.No, versions.No)
	h3 := newTestVersionHandler("1.5.0", "1.4.0", versions.No, versions.Yes)
	runUpgradeTest(
		t,
		func(opts service.Options) service.Options {
			return opts.WithCNOptionFunc(func(i int) []cnservice.Option {
				if i == 0 {
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
				}
				return []cnservice.Option{
					cnservice.WithBootstrapOptions(
						bootstrap.WithCheckUpgradeDuration(time.Millisecond*100),
						bootstrap.WithCheckUpgradeTenantDuration(time.Millisecond*100),
						bootstrap.WithCheckUpgradeTenantWorkers(1),
						bootstrap.WithUpgradeTenantBatch(30),
						bootstrap.WithUpgradeHandles([]bootstrap.VersionHandle{
							v1_2_0.Handler,
							h1,
							h2,
							h3,
						})),
				}
			})
		},
		func(c service.Cluster) {
			waitVersionReady(t, "1.2.0", c)
			checkVersionUpgrades(t, "1.2.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 0, len(upgrades))
			})
			createTenants(t, c, 100, "1.2.0")

			require.NoError(t, c.StartCNServices(1))
			waitVersionReady(t, "1.5.0", c)
			checkVersionUpgrades(t, "1.5.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 3, len(upgrades))
				checkVersionUpgrade(t, upgrades[0], "1.2.0", "1.3.0", "1.5.0", 0)
				checkVersionUpgrade(t, upgrades[1], "1.3.0", "1.4.0", "1.5.0", 0)
				checkVersionUpgrade(t, upgrades[2], "1.4.0", "1.5.0", "1.5.0", 101)
			})
			checkTenantVersion(t, c, "1.5.0")
		})
}

func checkVersionUpgrade(
	t *testing.T,
	upgrade versions.VersionUpgrade,
	from, to, final string,
	tenant int32) {
	require.Equal(t, from, upgrade.FromVersion)
	require.Equal(t, to, upgrade.ToVersion)
	require.Equal(t, final, upgrade.FinalVersion)
	require.Equal(t, versions.StateReady, upgrade.State)
	require.Equal(t, tenant, upgrade.TotalTenant)
	require.Equal(t, tenant, upgrade.ReadyTenant)
}
