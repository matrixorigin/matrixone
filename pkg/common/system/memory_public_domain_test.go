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

package system

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublicMemoryReadersUseOneDomainIncludingSoftOnly(t *testing.T) {
	previous := memoryDomainState.snapshot.Load()
	t.Cleanup(func() { memoryDomainState.snapshot.Store(previous) })
	for _, source := range []MemoryDomainSource{MemoryDomainSourceCgroupV1, MemoryDomainSourceCgroupV2, MemoryDomainSourceHost} {
		for _, hard := range []uint64{1000, math.MaxUint64} {
			s := MemoryDomainSnapshot{Used: 850, Limit: hard, SoftLimit: 900, Reclaimable: 100,
				Source: source, Measured: true, ObservedAt: time.Now().Add(time.Hour)}
			memoryDomainState.snapshot.Store(&s)
			require.Equal(t, uint64(900), MemoryTotal())
			require.Equal(t, uint64(850), MemoryUsed())
			require.Equal(t, uint64(50), MemoryAvailable())
			cacheAvailable, measured := MemoryAvailableIncludingCache()
			require.True(t, measured)
			require.Equal(t, uint64(150), cacheAvailable)
			wantHard := hard
			if source == MemoryDomainSourceHost || hard == math.MaxUint64 {
				wantHard = 0
			}
			require.Equal(t, wantHard, CgroupMemoryLimit())
			exhausted := s
			exhausted.Used = 901
			memoryDomainState.snapshot.Store(&exhausted)
			require.Zero(t, MemoryAvailable(), "usage above a lowered soft limit must not underflow")
		}
	}
}

func TestBindingHeadroomDoesNotLoseSmallerSizingOrCacheBounds(t *testing.T) {
	const gib = uint64(1 << 30)
	root := t.TempDir()
	child := filepath.Join(root, "child")
	require.NoError(t, os.Mkdir(child, 0o700))
	for _, level := range []struct {
		dir                string
		limit, used, cache uint64
	}{
		{root, 100 * gib, 99 * gib, 50 * gib},
		{child, 4 * gib, gib, 0},
	} {
		for name, value := range map[string]string{
			"memory.max": strconv.FormatUint(level.limit, 10), "memory.high": "max",
			"memory.current": strconv.FormatUint(level.used, 10),
			"memory.stat":    "file " + strconv.FormatUint(level.cache, 10) + "\ninactive_file " + strconv.FormatUint(level.cache, 10),
		} {
			require.NoError(t, os.WriteFile(filepath.Join(level.dir, name), []byte(value), 0o600))
		}
	}
	levels := buildMemoryDomainLevels(child, root, MemoryDomainSourceCgroupV2,
		"memory.max", "memory.high", "memory.current", "memory.stat")
	s, measured, err := readCgroupMemoryDomain(levels)
	require.NoError(t, err)
	require.True(t, measured)
	require.Equal(t, 100*gib, s.Limit)
	require.Equal(t, 99*gib, s.Used)
	require.Equal(t, 4*gib, s.CapacityLimit())
	require.Equal(t, gib, s.HardHeadroom())
	require.Equal(t, 3*gib, s.AdmissionReclaimableHeadroom(), "parent cache must not bypass the smaller child")
	previous := memoryDomainState.snapshot.Load()
	t.Cleanup(func() { memoryDomainState.snapshot.Store(previous) })
	s.ObservedAt = time.Now().Add(time.Hour)
	memoryDomainState.snapshot.Store(&s)
	require.Equal(t, 4*gib, MemoryTotal())
	require.Equal(t, 4*gib, CgroupMemoryLimit())
	require.Equal(t, gib, MemoryAvailable())
}
