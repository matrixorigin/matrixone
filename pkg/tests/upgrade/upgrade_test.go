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
							bootstrap.WithUpgradeHandles([]bootstrap.VersionHandle{
								v1_2_0.Handler,
							})),
					}
				}
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
			waitVersionReady(t, "1.2.0", c)
			checkVersionUpgrade(t, "1.2.0", c, func(upgrades []versions.VersionUpgrade) {
				require.Equal(t, 0, len(upgrades))
			})

		})
}
