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

package db

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/flusher"
	imem "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
)

var (
	DefaultFlushInterval     = time.Duration(20) * time.Second
	DefaultNodeFlushInterval = time.Duration(120) * time.Second
)

type flusherDriver struct {
	mgr     imem.IManager
	id      uint64
	checker func(int64) bool
}

func (driver *flusherDriver) GetId() uint64 {
	return driver.id
}

func (driver *flusherDriver) FlushNode(id uint64) error {
	c := driver.mgr.StrongRefTable(id)
	if c == nil {
		return nil
	}
	defer c.Unref()
	meta := c.GetMeta()
	if !driver.checker(meta.GetFlushTS()) {
		return nil
	}
	logutil.Infof("TimedFlushing | Shard-%d | Node-%d", driver.id, id)
	return c.Flush()
}

func createFlusherFactory(mgr imem.IManager) flusher.DriverFactory {
	return func(id uint64) flusher.NodeDriver {
		driver := &flusherDriver{
			mgr: mgr,
			id:  id,
			checker: func(ts int64) bool {
				return time.Now().UnixMicro()-ts > DefaultNodeFlushInterval.Microseconds()
			},
		}
		return driver
	}
}

type timedFlusherHandle struct {
	driver   flusher.Driver
	producer wal.ShardAwareWal
}

func (h *timedFlusherHandle) OnStopped() {
	logutil.Infof(h.driver.String())
}

func (h *timedFlusherHandle) OnExec() {
	entries := h.producer.GetAllPendingEntries()
	if entries == nil {
		return
	}
	h.driver.OnStats(entries)
}
