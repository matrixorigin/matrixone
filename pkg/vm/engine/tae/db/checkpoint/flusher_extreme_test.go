// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/stretchr/testify/require"
)

type capturedFlushPayload struct {
	data       []*catalog.ObjectEntry
	tombstones []*catalog.ObjectEntry
}

func captureFlushPayloads(t *testing.T) *[]capturedFlushPayload {
	t.Helper()
	original := jobs.FlushTableTailTaskFactory
	payloads := new([]capturedFlushPayload)
	jobs.FlushTableTailTaskFactory = func(data, tombstones []*catalog.ObjectEntry, _ *dbutils.Runtime) tasks.TxnTaskFactory {
		*payloads = append(*payloads, capturedFlushPayload{data: data, tombstones: tombstones})
		return nil
	}
	t.Cleanup(func() { jobs.FlushTableTailTaskFactory = original })
	return payloads
}

func assertPayloadObjects(t *testing.T, got []*catalog.ObjectEntry, all []*catalog.ObjectEntry, indices []int) {
	t.Helper()
	require.Len(t, got, len(indices))
	for i, index := range indices {
		require.Same(t, all[index], got[i], "payload position %d", i)
	}
}

func readyPrefix(mask []bool) int {
	for i, ready := range mask {
		if !ready {
			return i
		}
	}
	return len(mask)
}

// Every small readiness pattern must yield exactly the oldest contiguous
// prefix of each object kind. Inspect the task payload, not only its scopes.
func TestFlushFactoryPayloadExhaustiveSmallPrefixes(t *testing.T) {
	payloads := captureFlushPayloads(t)
	for dataMask := 0; dataMask < 8; dataMask++ {
		for tombMask := 0; tombMask < 4; tombMask++ {
			t.Run(fmt.Sprintf("data-%03b-tomb-%02b", dataMask, tombMask), func(t *testing.T) {
				dataReady := make([]bool, 3)
				tombReady := make([]bool, 2)
				for i := range dataReady {
					dataReady[i] = dataMask&(1<<i) != 0
				}
				for i := range tombReady {
					tombReady[i] = tombMask&(1<<i) != 0
				}
				f := newFlushSelectionFixture(t, dataReady, tombReady)
				*payloads = nil
				err := f.flusher.fireFlushTabletail(f.table, f.end(), types.TS{})
				nd, nt := readyPrefix(dataReady), readyPrefix(tombReady)
				deferred := nd < len(dataReady) || nt < len(tombReady)
				if deferred {
					require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "got %v", err)
				} else {
					require.NoError(t, err)
				}
				if nd+nt == 0 {
					require.Empty(t, *payloads)
					require.Empty(t, f.scheduler.scopes)
				} else {
					require.Len(t, *payloads, 1)
					require.Len(t, f.scheduler.scopes, 1)
					dataIndices := make([]int, nd)
					tombIndices := make([]int, nt)
					for i := range dataIndices {
						dataIndices[i] = i
					}
					for i := range tombIndices {
						tombIndices[i] = len(dataReady) + i
					}
					assertPayloadObjects(t, (*payloads)[0].data, f.metas, dataIndices)
					assertPayloadObjects(t, (*payloads)[0].tombstones, f.metas, tombIndices)
					scopes := append(f.scopes(dataIndices...), f.scopes(tombIndices...)...)
					require.Equal(t, scopes, f.scheduler.scopes[0])
				}
				for i, object := range f.objects {
					limit, at := nd, i
					if i >= len(dataReady) {
						limit, at = nt, i-len(dataReady)
					}
					wantCalls := 0
					if at <= limit {
						wantCalls = 1
					}
					require.Equal(t, wantCalls, object.calls, "object %d", i)
				}
			})
		}
	}
}

func TestFlushFactoryPayloadChunkBoundaryAndRetry(t *testing.T) {
	payloads := captureFlushPayloads(t)
	ready := make([]bool, 403)
	for i := range ready {
		ready[i] = true
	}
	ready[401] = false
	f := newFlushSelectionFixture(t, ready, []bool{true, true})
	f.scheduler.failAt = 2
	err := f.flusher.fireFlushTabletail(f.table, f.end(), types.TS{})
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "got %v", err)
	require.Len(t, *payloads, 2)
	require.Len(t, f.scheduler.scopes, 2)
	first := make([]int, 400)
	for i := range first {
		first[i] = i
	}
	assertPayloadObjects(t, (*payloads)[0].data, f.metas, first)
	assertPayloadObjects(t, (*payloads)[0].tombstones, f.metas, []int{403, 404})
	assertPayloadObjects(t, (*payloads)[1].data, f.metas, []int{400})
	require.Empty(t, (*payloads)[1].tombstones)
	require.Equal(t, f.scopes(append(first, 403, 404)...), f.scheduler.scopes[0])
	require.Equal(t, f.scopes(400), f.scheduler.scopes[1])
	require.Zero(t, f.objects[402].calls, "newer object must not be probed across busy boundary")
	retained := append([]capturedFlushPayload(nil), (*payloads)...)

	f.objects[401].ready = true
	f.scheduler.failAt = 0
	f.scheduler.scopes = nil
	*payloads = nil
	require.NoError(t, f.flusher.fireFlushTabletail(f.table, f.end(), types.TS{}))
	require.Len(t, *payloads, 2)
	second := []int{400, 401, 402}
	assertPayloadObjects(t, (*payloads)[0].data, f.metas, first)
	assertPayloadObjects(t, (*payloads)[0].tombstones, f.metas, []int{403, 404})
	assertPayloadObjects(t, (*payloads)[1].data, f.metas, second)
	require.Empty(t, (*payloads)[1].tombstones)
	require.Equal(t, f.scopes(append(first, 403, 404)...), f.scheduler.scopes[0])
	require.Equal(t, f.scopes(second...), f.scheduler.scopes[1])
	assertPayloadObjects(t, retained[1].data, f.metas, []int{400})
}

func TestFlushRejectedPartialPrefixRetainsNoTaskPayload(t *testing.T) {
	payloads := captureFlushPayloads(t)
	f := newFlushSelectionFixture(t, []bool{true, false, true}, []bool{true})
	calls := 0
	err := f.flusher.fireFlushTabletailWithPolicy(f.table, f.end(), types.TS{}, func(asize, dsize int) bool {
		calls++
		return false
	})
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "got %v", err)
	require.Equal(t, 1, calls)
	require.Empty(t, *payloads)
	require.Empty(t, f.scheduler.scopes)
	require.Zero(t, f.objects[2].calls)
}
