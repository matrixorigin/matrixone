package engine

import (
	e "matrixone/pkg/vm/engine/aoe/storage/event"
	"matrixone/pkg/vm/engine/aoe/storage/gc"
	"matrixone/pkg/vm/engine/aoe/storage/gc/gci"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	"sync"
	"time"
)

type IterOptions struct {
	TableName  string
	TableID    uint64
	All        bool
	ColIdxes   []int
	SegmentIds []uint64
}

type CacheCfg struct {
	IndexCapacity  uint64
	InsertCapacity uint64
	DataCapacity   uint64
}

type MetaCleanerCfg struct {
	Interval time.Duration
}

type Options struct {
	EventListener e.EventListener

	Mu sync.RWMutex

	Scheduler sched.Scheduler

	Meta struct {
		CKFactory *checkpointerFactory
		Conf      *md.Configuration
		Info      *md.MetaInfo
	}

	GC struct {
		Conf     *gci.WorkerCfg
		Acceptor gci.IAcceptor
	}

	CacheCfg *CacheCfg

	MetaCleanerCfg *MetaCleanerCfg
}

func (o *Options) FillDefaults(dirname string) *Options {
	if o == nil {
		o = &Options{}
	}
	o.EventListener.FillDefaults()

	if o.Meta.Conf == nil {
		o.Meta.Conf = &md.Configuration{
			BlockMaxRows:     md.BLOCK_ROW_COUNT,
			SegmentMaxBlocks: md.SEGMENT_BLOCK_COUNT,
			Dir:              dirname,
		}
	}
	if o.Meta.Info == nil {
		o.Meta.Info = md.NewMetaInfo(&o.Mu, o.Meta.Conf)
	}

	if o.Meta.CKFactory == nil {
		o.Meta.CKFactory = NewCheckpointerFactory(dirname)
	}

	if o.CacheCfg == nil {
		o.CacheCfg = &CacheCfg{
			IndexCapacity:  o.Meta.Conf.BlockMaxRows * o.Meta.Conf.SegmentMaxBlocks * 80,
			InsertCapacity: o.Meta.Conf.BlockMaxRows * o.Meta.Conf.SegmentMaxBlocks * 800,
			DataCapacity:   o.Meta.Conf.BlockMaxRows * o.Meta.Conf.SegmentMaxBlocks * 80,
		}
	}

	if o.GC.Acceptor == nil {
		cfg := o.GC.Conf
		if cfg == nil {
			cfg = new(gci.WorkerCfg)
			cfg.Interval = gci.DefaultInterval
		}
		o.GC.Acceptor = gc.NewWorker(cfg)
	}

	if o.MetaCleanerCfg == nil {
		o.MetaCleanerCfg = &MetaCleanerCfg{
			Interval: time.Duration(20) * time.Second,
		}
	}
	return o
}
