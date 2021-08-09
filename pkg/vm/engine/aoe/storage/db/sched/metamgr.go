package db

import (
	log "github.com/sirupsen/logrus"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type MetaResourceCfg struct {
}

type metaResourceMgr struct {
	sched.BaseResourceMgr
	disk sched.ResourceMgr
	cpu  sched.ResourceMgr
}

func NewMetaResourceMgr(disk, cpu sched.ResourceMgr) *metaResourceMgr {
	mgr := &metaResourceMgr{
		disk: disk,
		cpu:  cpu,
	}
	handler := sched.NewPoolHandler(4, mgr.preExec)
	mgr.BaseResourceMgr = *sched.NewBaseResourceMgr(handler)
	return mgr
}

func (mgr *metaResourceMgr) preExec(op iops.IOp) bool {
	log.Info("PreExec")
	return true
}
