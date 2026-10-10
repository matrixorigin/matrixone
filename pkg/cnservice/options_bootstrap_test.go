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

package cnservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_11"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_12"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/require"
)

func TestBootstrapOptionsPreserveCallerHandlers(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		s := &service{}
		WithBootstrapOptions(bootstrap.WithUpgradeHandles([]bootstrap.VersionHandle{v4_0_11.Handler}))(s)
		// NewService appends these after applying caller options.
		WithBootstrapOptions(bootstrap.WithUpgradeTenantBatch(16), bootstrap.WithKek("test"))(s)
		require.Len(t, s.options.bootstrapOptions, 3)
		check := func(version string) {
			b := bootstrap.NewService("", nil, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, nil, s.options.bootstrapOptions...)
			defer b.Close()
			require.Equal(t, version, b.GetFinalVersion())
		}
		check("4.0.11")
		// Options still apply in order: a later explicit handler list wins.
		WithBootstrapOptions(bootstrap.WithUpgradeHandles([]bootstrap.VersionHandle{v4_0_12.Handler}))(s)
		check("4.0.12")
	})
}
