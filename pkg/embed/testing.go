// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

var (
	basicOnce    sync.Once
	basicCluster Cluster
)

func init() {
	stats.SkipPanicONDuplicate.Store(true)
}

// RunBaseClusterTests starting an integration test for a 1 log, 1tn, 3cn base cluster is very slow
// due to the amount of time it takes to start a cluster (10-20s) when there are a very large number
// of test cases. So for some special cases that don't need to be restarted, a basicCluster can be
// reused to run the test cases. in summary, the basic cluster will only be started once!
func RunBaseClusterTests(
	fn func(Cluster),
) error {
	var err error
	var c Cluster
	basicOnce.Do(
		func() {
			c, err = NewCluster(
				WithCNCount(3),
			)
			if err != nil {
				return
			}
			err = c.Start()
			if err != nil {
				return
			}
			basicCluster = c
		},
	)
	if err != nil {
		return err
	}
	fn(basicCluster)
	return nil
}
