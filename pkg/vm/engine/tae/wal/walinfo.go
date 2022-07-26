package wal

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

type walInfo struct{
	ucLsnTidMap map[uint64]uint64
	cTidLsnMap map[uint64]uint64
}

func newWalInfo()*walInfo{
	return &walInfo{
		ucLsnTidMap: make(map[uint64]uint64),
		cTidLsnMap: make(map[uint64]uint64),
	}
}

func (w *walInfo)logEntry(info *entry.Info){
	if info.Group == GroupC {
		w.cTidLsnMap[info.TxnId] = info.GroupLSN
	}
	if info.Group == GroupUC {
		w.ucLsnTidMap[info.GroupLSN] = info.Uncommits
	}
}

func (w *walInfo)onLogInfo(items ...any){
	for _,item:=range items{
		e:=item.(*entryWithInfo)
		err:=e.e.WaitDone()
		if err!= nil{
			panic(err)
		}
		w.logEntry(e.info.(*entry.Info))
	}
}