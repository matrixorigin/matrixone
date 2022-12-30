// Copyright 2021 Matrix Origin
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

package batchstoredriver

import (
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

// wal ckping

type replayer struct {
	version int
	pos     int

	applyEntry driver.ApplyHandle

	//syncbase
	addrs  map[int]*common.ClosedIntervals
	maxlsn uint64
	minlsn uint64

	readDuration  time.Duration
	applyDuration time.Duration
}

func newReplayer(h driver.ApplyHandle) *replayer {
	return &replayer{
		addrs:      make(map[int]*common.ClosedIntervals),
		applyEntry: h,
		minlsn:     math.MaxUint64,
	}
}

func (r *replayer) updateaddrs(version int, lsn uint64) {
	interval, ok := r.addrs[version]
	if !ok {
		interval = common.NewClosedIntervals()
		r.addrs[version] = interval
	}
	interval.TryMerge(*common.NewClosedIntervalsByInt(lsn))
}

func (r *replayer) updateGroupLSN(lsn uint64) {
	if lsn > r.maxlsn {
		r.maxlsn = lsn
	}
	if lsn < r.minlsn {
		r.minlsn = lsn
	}
}

func (r *replayer) onReplayEntry(e *entry.Entry) error {
	e.SetInfo()
	info := e.Info
	if info == nil {
		return nil
	}
	r.updateaddrs(r.version, e.Lsn)
	r.updateGroupLSN(e.Lsn)
	return nil
}

func (r *replayer) replayHandler(vfile *vFile) error {
	if vfile.version != r.version {
		r.pos = 0
		r.version = vfile.version
	}
	e := entry.NewEmptyEntry()
	t0 := time.Now()
	_, err := e.ReadAt(vfile.File, r.pos)
	r.readDuration += time.Since(t0)
	if err != nil {
		return err
	}
	if err := r.onReplayEntry(e); err != nil {
		return err
	}
	vfile.onReplay(r.pos, e.Lsn)
	t0 = time.Now()
	r.applyEntry(e)
	r.applyDuration += time.Since(t0)
	r.pos += e.GetSize()
	e.Entry.Free()
	return nil
}
