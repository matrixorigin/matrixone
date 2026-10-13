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

package docfilter_test

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/stretchr/testify/require"
)

// Keep the production-policy consumer in an external test package: the CN
// throttler depends on ObjectIO, whose scoped filter operations use docfilter.
func TestMemoryAdmissionUsesProductionParallelPolicy(t *testing.T) {
	const (
		request    = int64(1 << 20)
		limitSlots = int64(10)
		// The production CN policy's hard cap is 80% of the pool.
		grantedSlots = int64(8)
		workers      = 100
	)
	admission := rscthrottler.NewMemThrottler(
		"docfilter-parallel-test",
		1,
		rscthrottler.WithConstLimit(request*limitSlots),
		rscthrottler.WithAcquirePolicy(
			rscthrottler.AcquirePolicyForCNFlushS3),
	)

	start := make(chan struct{})
	releaseAll := make(chan struct{})
	results := make(chan bool, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, err := docfilter.AcquireMemoryForTest(admission, request)
			if err != nil {
				results <- false
				return
			}
			results <- true
			<-releaseAll
			release()
		}()
	}
	close(start)
	granted := 0
	for range workers {
		if <-results {
			granted++
		}
	}
	close(releaseAll)
	wg.Wait()
	require.Equal(t, int(grantedSlots), granted)
	require.Equal(t, request*limitSlots, admission.Available())
}
