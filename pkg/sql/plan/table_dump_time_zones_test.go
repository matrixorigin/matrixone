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

package plan

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTableDumpTimeZoneSnapshot(t *testing.T) {
	t.Parallel()
	const name = "Africa/Abidjan"
	rules := time.FixedZone("initial rules", 3600)
	available := true
	calls := 0
	load := func(candidate string) (*time.Location, error) {
		calls++
		if candidate == name && available {
			return rules, nil
		}
		return nil, errors.New("unavailable timezone")
	}
	firstNamed := func(zones func(func(*time.Location) bool)) *time.Location {
		t.Helper()
		var result *time.Location
		count := 0
		zones(func(zone *time.Location) bool {
			count++
			switch count {
			case 1:
				require.Same(t, time.UTC, zone)
			case 2:
				require.Same(t, time.Local, zone)
			case 3:
				result = zone
				return false
			}
			return true
		})
		return result
	}
	snapshot := newTableDumpTimeZones(load)
	original := rules
	require.Same(t, original, firstNamed(snapshot))
	loadedCalls := calls
	require.Equal(t, 1, loadedCalls, "an early witness must not load later zones")
	rules = time.FixedZone("updated rules", 7200)
	require.Same(t, original, firstNamed(snapshot), "one verification retains its rule snapshot")
	require.Equal(t, loadedCalls, calls, "repeated candidates reuse loaded rules")
	require.Same(t, rules, firstNamed(newTableDumpTimeZones(load)), "the next verification reloads rules")

	available = false
	missing := newTableDumpTimeZones(load)
	require.NotSame(t, rules, firstNamed(missing), "missing named rules fall through to fixed offsets")
	failedCalls := calls
	available = true
	require.NotSame(t, rules, firstNamed(missing), "failure belongs to this verification snapshot")
	require.Equal(t, failedCalls, calls)
	require.Same(t, rules, firstNamed(newTableDumpTimeZones(load)), "a later verification retries a failed load")
}

func TestTableDumpTimeZoneLazyResume(t *testing.T) {
	t.Parallel()
	var loaded []string
	zones := newTableDumpTimeZones(func(name string) (*time.Location, error) {
		loaded = append(loaded, name)
		if name == "Africa/Accra" {
			return time.UTC, errors.New("unavailable timezone")
		}
		return time.FixedZone(name, 3600), nil
	})
	visit := func(limit int) []string {
		var visited []string
		zones(func(zone *time.Location) bool {
			visited = append(visited, zone.String())
			return len(visited) < limit
		})
		return visited
	}
	visit(1)
	visit(2)
	require.Empty(t, loaded, "UTC and Local matches need no named rule loads")
	first := visit(3)
	require.Equal(t, []string{"Africa/Abidjan"}, loaded)
	resumed := visit(4)
	require.Equal(t, first, resumed[:3])
	require.Equal(t, "Africa/Addis_Ababa", resumed[3])
	require.Equal(t, []string{"Africa/Abidjan", "Africa/Accra", "Africa/Addis_Ababa"}, loaded)
	require.Equal(t, resumed, visit(4))
	require.Len(t, loaded, 3, "reuse both successful rules and failed loads")
}

func BenchmarkTableDumpTimeZoneEarlyWitness(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		zones := newTableDumpTimeZones(time.LoadLocation)
		count := 0
		zones(func(*time.Location) bool {
			count++
			return count < 5 // UTC, Local and the first three named candidates.
		})
	}
}

func TestTableDumpFixedTimeZoneWitnesses(t *testing.T) {
	t.Parallel()
	zones := newTableDumpTimeZones(func(string) (*time.Location, error) {
		return nil, errors.New("no named rules")
	})
	seen := make(map[int]bool)
	count := 0
	zones(func(zone *time.Location) bool {
		count++
		if count <= 2 {
			return true
		}
		_, offset := time.Unix(0, 0).In(zone).Zone()
		require.NotZero(t, offset)
		require.False(t, seen[offset], "duplicate fixed offset %d", offset)
		seen[offset] = true
		return true
	})
	require.Len(t, seen, 1679)
	for minute := -(13*60 + 59); minute <= 14*60; minute++ {
		if minute != 0 {
			require.True(t, seen[minute*60], "missing offset %d", minute)
		}
	}
}
