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

package issues

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

var authenticatedClusterSuite struct {
	once    sync.Once
	running sync.Mutex
	cluster embed.Cluster
	err     error
}

func runAuthenticatedClusterTest(t *testing.T, fn func(embed.Cluster)) {
	t.Helper()

	authenticatedClusterSuite.running.Lock()
	defer authenticatedClusterSuite.running.Unlock()

	authenticatedClusterSuite.once.Do(func() {
		authenticatedClusterSuite.cluster, authenticatedClusterSuite.err = embed.NewCluster(
			embed.WithCNCount(1),
			embed.WithTesting(),
			embed.WithPreStart(func(svc embed.ServiceOperator) {
				if svc.ServiceType() != metadata.ServiceType_CN {
					return
				}
				svc.Adjust(func(cfg *embed.ServiceConfig) {
					cfg.CN.LockService.MaxFixedSliceSize = 10001
					cfg.CN.LockService.MaxLockRowCount = 10000
					cfg.CN.Frontend.SkipCheckUser = false
				})
			}),
		)
		if authenticatedClusterSuite.err != nil {
			return
		}
		authenticatedClusterSuite.err = authenticatedClusterSuite.cluster.Start()
	})

	require.NoError(t, authenticatedClusterSuite.err)
	fn(authenticatedClusterSuite.cluster)
}

func TestMain(m *testing.M) {
	code := m.Run()
	if authenticatedClusterSuite.cluster != nil {
		if err := authenticatedClusterSuite.cluster.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close authenticated test cluster: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}
