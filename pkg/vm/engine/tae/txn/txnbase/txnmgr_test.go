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

package txnbase

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestTryUpdateMaxCommittedTSNeverMovesBackward(t *testing.T) {
	mgr := &TxnManager{}
	mgr.initMaxCommittedTS()

	newer := types.BuildTS(2, 0)
	older := types.BuildTS(1, 0)
	mgr.TryUpdateMaxCommittedTS(newer)
	mgr.TryUpdateMaxCommittedTS(older)

	require.Equal(t, newer, *mgr.MaxCommittedTS.Load())
}

func TestTryUpdateMaxCommittedTSConcurrent(t *testing.T) {
	mgr := &TxnManager{}
	mgr.initMaxCommittedTS()

	const updates = 100
	var wg sync.WaitGroup
	for i := 1; i <= updates; i++ {
		ts := types.BuildTS(int64(i), 0)
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.TryUpdateMaxCommittedTS(ts)
		}()
	}
	wg.Wait()

	require.Equal(t, types.BuildTS(updates, 0), *mgr.MaxCommittedTS.Load())
}
