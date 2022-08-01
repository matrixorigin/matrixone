package wal

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type walInfo struct {
	ucLsnTidMap map[uint64]uint64
	ucmu        sync.RWMutex
	cTidLsnMap  map[uint64]uint64
	cmu         sync.RWMutex
}

func newWalInfo() *walInfo {
	return &walInfo{
		ucLsnTidMap: make(map[uint64]uint64),
		ucmu:        sync.RWMutex{},
		cTidLsnMap:  make(map[uint64]uint64),
		cmu:         sync.RWMutex{},
	}
}

func (w *walInfo) logEntry(info *entry.Info) {
	if info.Group == GroupC {
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
