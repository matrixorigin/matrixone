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

package wal

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
)

type walDriver struct {
	sync.RWMutex
	impl store.Store
	own  bool
}

func NewDriver(dir, name string, cfg *store.StoreCfg) Driver {
	impl, err := store.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	driver := NewDriverWithStore(impl, true)
	return driver
}

func NewDriverWithStore(impl store.Store, own bool) Driver {
	driver := new(walDriver)
	driver.impl = impl
	driver.own = own
	return driver
}

func (driver *walDriver) GetCheckpointed() uint64 {
	return driver.impl.GetCheckpointed(GroupC)
}

func (driver *walDriver) Checkpoint(indexes []*Index) (e LogEntry, err error) {
	// for _, index := range indexes {
	// 	logutil.Infof("Checkpoint Index: %s", index.String())
	// }
	commands := make(map[uint64]entry.CommandInfo)
	for _, idx := range indexes {
		cmdInfo, ok := commands[idx.LSN]
		if !ok {
			cmdInfo = entry.CommandInfo{
				CommandIds: []uint32{idx.CSN},
				Size:       idx.Size,
			}
		} else {
			existed := false
			for _, csn := range cmdInfo.CommandIds {
				if csn == idx.CSN {
					existed = true
					break
				}
			}
			if existed {
				continue
			}
			cmdInfo.CommandIds = append(cmdInfo.CommandIds, idx.CSN)
			if cmdInfo.Size != idx.Size {
				panic("logic error")
			}
		}
		commands[idx.LSN] = cmdInfo
	}
	info := &entry.Info{
		Group: entry.GTCKp,
		Checkpoints: []entry.CkpRanges{{
			Group:   GroupC,
			Command: commands,
		}},
	}
	e = entry.GetBase()
	e.SetType(entry.ETCheckpoint)
	e.SetInfo(info)
	_, err = driver.impl.AppendEntry(entry.GTCKp, e)
	return
}

func (driver *walDriver) Compact() error {
	return driver.impl.TryCompact()
}

func (driver *walDriver) GetPenddingCnt() uint64 {
	return driver.impl.GetPenddingCnt(GroupC)
}

func (driver *walDriver) GetCurrSeqNum() uint64 {
	return driver.impl.GetCurrSeqNum(GroupC)
}

func (driver *walDriver) LoadEntry(groupId uint32, lsn uint64) (LogEntry, error) {
	return driver.impl.Load(groupId, lsn)
}

func (driver *walDriver) AppendEntry(group uint32, e LogEntry) (uint64, error) {
	id, err := driver.impl.AppendEntry(group, e)
	return id, err
}

func (driver *walDriver) Close() error {
	if driver.own {
		return driver.impl.Close()
	}
	return nil
}
