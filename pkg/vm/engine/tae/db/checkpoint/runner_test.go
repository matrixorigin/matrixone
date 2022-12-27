// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestCkpCheck(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(nil, nil, nil, nil, nil)

	for i := 0; i < 100; i += 10 {
		r.storage.entries.Set(&CheckpointEntry{
			start:    types.BuildTS(int64(i), 0),
			end:      types.BuildTS(int64(i+9), 0),
			state:    ST_Finished,
			location: fmt.Sprintf("loc-%d", i),
		})
	}

	r.storage.entries.Set(&CheckpointEntry{
		start:    types.BuildTS(int64(100), 0),
		end:      types.BuildTS(int64(109), 0),
		state:    ST_Running,
		location: "loc-100",
	})

	loc, e := r.CollectCheckpointsInRange(types.BuildTS(4, 0), types.BuildTS(5, 0))
	assert.True(t, e.Equal(types.BuildTS(9, 0)))
	assert.Equal(t, "loc-0", loc)

	loc, e = r.CollectCheckpointsInRange(types.BuildTS(12, 0), types.BuildTS(25, 0))
	assert.True(t, e.Equal(types.BuildTS(29, 0)))
	assert.Equal(t, "loc-10;loc-20", loc)
}

func TestGetCheckpoints1(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(nil, nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		entry := &CheckpointEntry{
			start:    timestamps[i].Next(),
			end:      timestamps[i+1],
			state:    ST_Finished,
			location: fmt.Sprintf("ckp%d", i),
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.storage.entries.Set(entry)
	}

	// [0,10]
	location, checkpointed := r.CollectCheckpointsInRange(types.BuildTS(0, 1), types.BuildTS(10, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp0", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(10, 0)))

	// [45,50]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(45, 0), types.BuildTS(50, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "", location)
	assert.True(t, checkpointed.IsEmpty())

	// [30,45]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(30, 1), types.BuildTS(45, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [25,45]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(25, 1), types.BuildTS(45, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;ckp3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [22,25]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(22, 1), types.BuildTS(25, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(30, 0)))

	// [22,35]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(22, 1), types.BuildTS(35, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;ckp3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))
}
func TestGetCheckpoints2(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(nil, nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// global3[0,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		addGlobal := false
		if i == 3 {
			addGlobal = true
		}
		if addGlobal {
			entry := &CheckpointEntry{
				start:    types.TS{},
				end:      timestamps[i].Next(),
				state:    ST_Finished,
				location: fmt.Sprintf("global%d", i),
			}
			r.storage.globals.Set(entry)
		}
		start := timestamps[i].Next()
		if addGlobal {
			start = start.Next()
		}
		entry := &CheckpointEntry{
			start:    start,
			end:      timestamps[i+1],
			state:    ST_Finished,
			location: fmt.Sprintf("ckp%d", i),
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.storage.entries.Set(entry)
	}

	// [0,10]
	location, checkpointed := r.CollectCheckpointsInRange(types.BuildTS(0, 1), types.BuildTS(10, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(30, 1)))

	// [45,50]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(45, 0), types.BuildTS(50, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "", location)
	assert.True(t, checkpointed.IsEmpty())

	// [30,45]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(30, 2), types.BuildTS(45, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [25,45]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(25, 1), types.BuildTS(45, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;ckp3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [22,25]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(22, 1), types.BuildTS(25, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(30, 1)))

	// [22,35]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(22, 1), types.BuildTS(35, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;ckp3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [22,29]
	location, checkpointed = r.CollectCheckpointsInRange(types.BuildTS(22, 1), types.BuildTS(29, 0))
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(30, 1)))
}
func TestICKPSeekLT(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(nil, nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		entry := &CheckpointEntry{
			start:    timestamps[i].Next(),
			end:      timestamps[i+1],
			state:    ST_Finished,
			location: fmt.Sprintf("ckp%d", i),
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.storage.entries.Set(entry)
	}

	// 0, 1
	ckps := r.ICKPSeekLT(types.BuildTS(0, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].location)

	// 0, 0
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 0)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 0, 2
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 4)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].location)
	assert.Equal(t, "ckp1", ckps[1].location)

	// 0, 4
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 4)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].location)
	assert.Equal(t, "ckp1", ckps[1].location)
	assert.Equal(t, "ckp2", ckps[2].location)
	assert.Equal(t, "ckp3", ckps[3].location)

	// 0,10
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 10)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].location)
	assert.Equal(t, "ckp1", ckps[1].location)
	assert.Equal(t, "ckp2", ckps[2].location)
	assert.Equal(t, "ckp3", ckps[3].location)

	// 5,1
	ckps = r.ICKPSeekLT(types.BuildTS(5, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp1", ckps[0].location)

	// 50,1
	ckps = r.ICKPSeekLT(types.BuildTS(50, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 55,3
	ckps = r.ICKPSeekLT(types.BuildTS(55, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 40,3
	ckps = r.ICKPSeekLT(types.BuildTS(40, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 35,3
	ckps = r.ICKPSeekLT(types.BuildTS(35, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 30,3
	ckps = r.ICKPSeekLT(types.BuildTS(30, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp3", ckps[0].location)

	// 30-2,3
	ckps = r.ICKPSeekLT(types.BuildTS(30, 2), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))
}
