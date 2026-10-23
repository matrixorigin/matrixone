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

package fileservice

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/require"
)

type attributionRejectedData struct{ fscache.Data }

func (*attributionRejectedData) CacheAdmissionAllowed(*fscache.DataOwner) bool { return false }

type attributionRejectedSet struct{ fscache.DataCache }

func (*attributionRejectedSet) Set(context.Context, fscache.CacheKey, fscache.Data) (bool, error) {
	return false, fscache.ErrCacheAdmissionRejected
}

func TestMemoryCacheUpdateAttribution(t *testing.T) {
	for _, scenario := range []string{"admitted", "already_present", "policy", "before_rehome", "after_rehome", "set_rejected", "cache_hit", "empty"} {
		t.Run(scenario, func(t *testing.T) {
			var counters perfcounter.CounterSet
			ctx := perfcounter.WithCounterSet(context.Background(), &counters)
			cache := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, t.Name())
			t.Cleanup(func() { cache.Close(ctx) })
			size := 1
			if scenario == "after_rehome" {
				size = 2 << 20
			}
			data := NewBytes(make([]byte, size))
			vec := &IOVector{FilePath: "shared:/cache-attribution", Entries: []IOEntry{{Size: int64(size), CachedData: data}}}
			t.Cleanup(vec.Release)
			switch scenario {
			case "already_present":
				require.NoError(t, cache.Update(ctx, vec, false))
				counters.Reset()
			case "policy":
				vec.Policy = SkipMemoryCacheWrites
			case "before_rehome":
				vec.Entries[0].CachedData = &attributionRejectedData{Data: data}
			case "set_rejected":
				cache.cache = &attributionRejectedSet{DataCache: cache.cache}
			case "cache_hit":
				vec.Entries[0].fromCache = cache
			case "empty":
				vec.Entries[0].CachedData = nil
				data.Release()
			}
			require.NoError(t, cache.Update(ctx, vec, false))
			memory := &counters.FileService.Cache.Memory
			if scenario == "policy" {
				require.EqualValues(t, 1, memory.UpdatePolicySkips.Load())
			} else {
				require.Zero(t, memory.UpdatePolicySkips.Load())
			}
			if scenario == "policy" || scenario == "cache_hit" || scenario == "empty" {
				require.Zero(t, memory.UpdateEntries.Load())
				return
			}
			require.EqualValues(t, 1, memory.UpdateEntries.Load())
			switch scenario {
			case "admitted":
				require.EqualValues(t, 1, memory.UpdateAdmitted.Load())
			case "already_present":
				require.EqualValues(t, 1, memory.UpdateAlreadyPresent.Load())
			default:
				require.EqualValues(t, 1, memory.UpdatePressureSkips.Load())
			}
			require.Equal(t, memory.UpdateEntries.Load(), memory.UpdateAdmitted.Load()+memory.UpdateAlreadyPresent.Load()+memory.UpdatePressureSkips.Load())
		})
	}
}
