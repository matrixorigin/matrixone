// Copyright 2024 Matrix Origin
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

package merge

import (
	"context"
	"math"
	"os"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceController(t *testing.T) {
	rc := new(resourceController)
	rc.setMemLimit(10000)
	require.Equal(t, int64(7500), rc.limit)
	require.Equal(t, int64(7500), rc.availableMem())

	rc.refresh()
	rc.limit = rc.using + 1
	require.Equal(t, int64(1), rc.availableMem())

	require.Panics(t, func() { rc.setMemLimit(0) })

	objs := []*catalog.ObjectEntry{
		newTestObjectEntry(t, 0, false),
		newTestVarcharObjectEntry(t, "", "", 2),
	}
	rc.reserveResources(int64(mergesort.EstimateMergeSize(IterEntryAsStats(objs))))
	require.Greater(t, rc.reserved, int64(120))
}

func Test_CleanUpUselessFiles(t *testing.T) {
	tDir := os.TempDir()
	dir := path.Join(tDir, "/local")
	assert.NoError(t, os.RemoveAll(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	c := fileservice.Config{
		Name:    defines.ETLFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fs, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	ent := &api.MergeCommitEntry{
		BookingLoc: []string{"abc"},
	}

	CleanUpUselessFiles(ent, fs)
}

func TestRemoveOversize(t *testing.T) {
	o1 := newSortedTestObjectEntry(t, 0, 0, 1)
	o2 := newSortedTestObjectEntry(t, 0, 0, 2)
	o3 := newSortedTestObjectEntry(t, 0, 0, 4)
	o5 := newSortedTestObjectEntry(t, 0, 0, math.MaxInt32)

	require.ElementsMatch(t, []*catalog.ObjectEntry{o1, o2}, removeOversize([]*catalog.ObjectEntry{o1, o2}))
	require.ElementsMatch(t, []*catalog.ObjectEntry{o1, o2}, removeOversize([]*catalog.ObjectEntry{o5, o1, o2}))
	require.ElementsMatch(t, nil, removeOversize([]*catalog.ObjectEntry{o1, o3}))

	os := []*catalog.ObjectEntry{}
	sizes := []uint32{}
	for i := 0; i < 100; i++ {
		sizes = append(sizes, uint32(i+1000))
	}
	sizes[0] = 1
	sizes[1] = 10
	sizes[2] = 100
	for i := 0; i < 100; i++ {
		os = append(os, newSortedTestObjectEntry(t, 0, 0, sizes[i]))
	}
	require.ElementsMatch(t, []*catalog.ObjectEntry{os[0], os[1]}, removeOversize(os))
}

func BenchmarkRemoveOversize(b *testing.B) {
	o1 := newSortedTestObjectEntry(b, 0, 50, math.MaxInt32)
	o2 := newSortedTestObjectEntry(b, 51, 100, 1)
	o3 := newSortedTestObjectEntry(b, 49, 52, 2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		removeOversize([]*catalog.ObjectEntry{o1, o2, o3})
	}
}

func TestMergeSettings(t *testing.T) {
	settings := DefaultMergeSettings.Clone()
	settings.L0MaxCountDecayControl = []float64{0.1, 0.2, 0.3, 0.4}
	t.Log(settings.String())
	trigger, err := settings.Clone().ToMMsgTaskTrigger()
	require.NoError(t, err)
	require.Equal(t, [4]float64{0.1, 0.2, 0.3, 0.4}, trigger.l0.CPoints)

	{
		settings := DefaultMergeSettings.Clone()
		settings.TombstoneL1Size = "100xx"
		_, err = settings.ToMMsgTaskTrigger()
		require.Error(t, err)
		settings.TombstoneL1Size = "100b"
		_, err = settings.ToMMsgTaskTrigger()
		require.NoError(t, err)

		settings.VacuumScoreDecayDuration = "bad2m1s"
		_, err = settings.ToMMsgTaskTrigger()
		require.Error(t, err)

		settings.VacuumScoreDecayDuration = "2m1s"
		_, err = settings.ToMMsgTaskTrigger()
		require.NoError(t, err)

		settings.L0MaxCountDecayDuration = "bad2m1s"
		_, err = settings.ToMMsgTaskTrigger()
		require.Error(t, err)

		settings.L0MaxCountDecayDuration = "2m1s"
		_, err = settings.ToMMsgTaskTrigger()
		require.NoError(t, err)
	}
}
