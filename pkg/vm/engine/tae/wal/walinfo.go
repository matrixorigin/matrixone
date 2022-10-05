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
)

type walInfo struct {
	ucLsnTidMap map[uint64]string
	ucmu        sync.RWMutex
	cTidLsnMap  map[string]uint64
	cmu         sync.RWMutex
}

func newWalInfo() *walInfo {
	return &walInfo{
		ucLsnTidMap: make(map[uint64]string),
		ucmu:        sync.RWMutex{},
		cTidLsnMap:  make(map[string]uint64),
		cmu:         sync.RWMutex{},
	}
}

func (w *walInfo) logEntry(info *entry.Info) {
	if info.Group == GroupPrepare {
		w.cmu.Lock()
		w.cTidLsnMap[info.TxnId] = info.GroupLSN
		w.cmu.Unlock()
	}
	if info.Group == GroupUC {
		w.ucmu.Lock()
		w.ucLsnTidMap[info.GroupLSN] = info.Uncommits
		w.ucmu.Unlock()
	}
}

func (w *walInfo) onLogInfo(items ...any) {
	for _, item := range items {
		e := item.(*entryWithInfo)
		w.logEntry(e.info.(*entry.Info))
	}
}
