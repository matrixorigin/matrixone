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

//go:build linux

package rscthrottler

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnifiedHierarchyConstrainsAdmission(t *testing.T) {
	const MiB = int64(1 << 20)
	for _, test := range []struct {
		name, leafLimit string
		parentUsage     int64
		wantLimit       int64
		wantFree        int64
	}{
		{"unlimited leaf", "max", 32 * MiB, 512 * MiB, 480 * MiB},
		{"sibling pressure", fmt.Sprint(256 * MiB), 480 * MiB, 256 * MiB, 32 * MiB},
	} {
		t.Run(test.name, func(t *testing.T) {
			mount := t.TempDir()
			parent := filepath.Join(mount, "pod")
			leaf := filepath.Join(parent, "cn")
			require.NoError(t, os.MkdirAll(leaf, 0700))
			write := func(dir, name, value string) {
				t.Helper()
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(value), 0600))
			}
			const stat = "inactive_file 0\nfile_dirty 0\nfile_writeback 0\n"
			write(mount, "memory.stat", stat) // The global root has no memory.max/current.
			write(parent, "memory.stat", stat)
			write(parent, "memory.max", fmt.Sprint(512*MiB))
			write(parent, "memory.current", fmt.Sprint(test.parentUsage))
			write(leaf, "memory.stat", stat)
			write(leaf, "memory.max", test.leafLimit)
			write(leaf, "memory.current", fmt.Sprint(32*MiB))

			m, c, _ := admissionFixture(t, 64<<30, 50<<30, 32*MiB)
			c.readSample = func() memorySample {
				s := memorySample{total: 64 << 30, rss: 32 * MiB}
				s.cgroupErr = readUnifiedMemoryHierarchy(&s, leaf, mount)
				s.cgroupObserved = s.cgroupErr == nil
				return s
			}
			m.ForceRefresh()
			require.Equal(t, WorkingSetHeadroomSnapshot{Limit: test.wantLimit, Available: test.wantFree}, m.WorkingSetHeadroom())
			w, granted, err := m.TryReserveWorkingSet(context.Background(), candidateSpec(MiB, test.wantFree+1, 0))
			require.NoError(t, err)
			require.False(t, granted)
			require.Nil(t, w)
			w, granted, err = m.TryReserveWorkingSet(context.Background(), candidateSpec(MiB, test.wantFree, 0))
			require.NoError(t, err)
			require.True(t, granted)
			w.Close()

			require.NoError(t, os.Remove(filepath.Join(parent, "memory.current")))
			c.invalidateSample()
			m.ForceRefresh()
			require.Equal(t, WorkingSetHeadroomSnapshot{Limit: test.wantLimit}, m.WorkingSetHeadroom())
			require.False(t, m.physicalFits(1), "a partial ancestor sample cannot release the limit")
		})
	}
}

func TestUnifiedMemoryDistinguishesAbsentAndIncompleteControllers(t *testing.T) {
	path := t.TempDir()
	_, _, _, err := readUnifiedMemory(path)
	require.ErrorIs(t, err, errNoMemoryController)
	write := func(name, value string) {
		require.NoError(t, os.WriteFile(filepath.Join(path, name), []byte(value), 0600))
	}
	write("memory.max", "2200\n")
	_, _, _, err = readUnifiedMemory(path)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNoMemoryController, "a partial accounting sample cannot select host-memory fallback")
	write("memory.current", "1900\n")
	write("memory.stat", "inactive_file 500\nfile_dirty 100\nfile_writeback 50\n")
	limit, current, inactive, err := readUnifiedMemory(path)
	require.NoError(t, err)
	require.Equal(t, int64(2200), limit)
	require.Equal(t, int64(1900), current)
	require.Equal(t, int64(350), inactive)
	write("memory.max", "max\n")
	limit, _, _, err = readUnifiedMemory(path)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), limit)
	write("memory.stat", "inactive_file 500\n")
	_, _, inactive, err = readUnifiedMemory(path)
	require.NoError(t, err)
	require.Zero(t, inactive, "omitted dirty/writeback counters cannot create reclaim credit")
	write("memory.current", "broken\n")
	_, _, _, err = readUnifiedMemory(path)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNoMemoryController)
	require.NoError(t, os.Remove(filepath.Join(path, "memory.current")))
	require.NoError(t, os.Mkdir(filepath.Join(path, "memory.current"), 0700))
	_, _, _, err = readUnifiedMemory(path)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNoMemoryController, "I/O errors must remain errors even before first successful controller sample")
}
