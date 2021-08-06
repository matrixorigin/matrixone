package engine

import (
	e "matrixone/pkg/vm/engine/aoe/storage/event"
	"matrixone/pkg/vm/engine/aoe/storage/gc"
	"matrixone/pkg/vm/engine/aoe/storage/gc/gci"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sync"
	"time"
)

const (
	DEFAULT_MON_COLLECTOR = "MON_COLLECTOR"
	DEFAULT_META_FLUSHER  = "META_FLUSER"
	DEFAULT_META_UPDATER  = "META_UPDATER"
	DEFAULT_DATA_FLUSHER  = "DATA_FLUSHER"
	DEFAULT_DATA_SORTER   = "DATA_SORTER"
	DEFAULT_MDATA_UPDATER = "MDATA_UPDATER"
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

	Mon struct {
		Collector iw.IOpWorker
	}

	Meta struct {
		Flusher      iw.IOpWorker
		Updater      iw.IOpWorker
		Checkpointer *Checkpointer
		Conf         *md.Configuration
		Info         *md.MetaInfo
	}

	Data struct {
		Flusher iw.IOpWorker
		Sorter  iw.IOpWorker
		// IOFactory ioif.IOFactory
	}

	MemData struct {
		Updater iw.IOpWorker
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

	if o.Mon.Collector == nil {
		o.Mon.Collector = w.NewOpWorker(DEFAULT_MON_COLLECTOR)
	}

	if o.Meta.Flusher == nil {
		o.Meta.Flusher = w.NewOpWorker(DEFAULT_META_FLUSHER)
	}
	if o.Meta.Updater == nil {
		o.Meta.Updater = w.NewOpWorker(DEFAULT_META_UPDATER)
	}
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

	if o.Meta.Checkpointer == nil {
		o.Meta.Checkpointer = NewCheckpointer(o, dirname)
	}

	// if o.Data.IOFactory == nil {
	// 	dio.WRITER_FACTORY.Opts = o
	// 	dio.WRITER_FACTORY.Dirname = dirname
	// 	dio.READER_FACTORY.Opts = o
	// 	dio.READER_FACTORY.Dirname = dirname
	// o.Data.IOFactory = WRITER_FACTORY
	// o.Data.WriterFactory.Opts = o
	// o.Data.WriterFactory.Dirname = dirname
	// }

	if o.Data.Flusher == nil {
		o.Data.Flusher = w.NewOpWorker(DEFAULT_DATA_FLUSHER)
	}

	if o.Data.Sorter == nil {
		o.Data.Sorter = w.NewOpWorker(DEFAULT_DATA_SORTER)
	}

	if o.MemData.Updater == nil {
		o.MemData.Updater = w.NewOpWorker(DEFAULT_MDATA_UPDATER)
	}

	if o.CacheCfg == nil {
		o.CacheCfg = &CacheCfg{
			IndexCapacity:  o.Meta.Conf.BlockMaxRows * o.Meta.Conf.SegmentMaxBlocks * 80,
			InsertCapacity: o.Meta.Conf.BlockMaxRows * o.Meta.Conf.SegmentMaxBlocks * 80,
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
