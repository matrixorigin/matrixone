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

package storage

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/event"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc/gci"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
)

const (
	DefaultIndexCacheSize = 128 * common.M
	DefaultMTCacheSize    = 4 * common.G
	DefaultSSTCacheSize   = 4 * common.G

	DefaultBlockMaxRows     = uint64(40000)
	DefaultBlocksPerSegment = uint64(40)

	DefaultCleanInterval = 5

	DefaultBlockWriters     = uint16(8)
	DefaultSegmentWriters   = uint16(4)
	DefaultIndexWriters     = uint16(4)
	DefaultStatelessWorkers = uint16(1)
)

type IterOptions struct {
	TableName  string
	TableID    uint64
	All        bool
	ColIdxes   []int
	SegmentIds []uint64
}

type CacheCfg struct {
	IndexCapacity  uint64 `toml:"index-cache-size"`
	InsertCapacity uint64 `toml:"insert-cache-size"`
	DataCapacity   uint64 `toml:"data-cache-size"`
}

type MetaCfg struct {
	BlockMaxRows     uint64 `toml:"block-max-rows"`
	SegmentMaxBlocks uint64 `toml:"segment-max-blocks"`
}

type SchedulerCfg struct {
	BlockWriters     uint16 `toml:"block-writers"`
	SegmentWriters   uint16 `toml:"segment-writers"`
	IndexWriters     uint16 `toml:"index-writers"`
	StatelessWorkers uint16 `toml:"stateless-workers"`
}

type MetaCleanerCfg struct {
	Interval time.Duration
}

type Options struct {
	EventListener event.EventListener

	Mu sync.RWMutex

	Scheduler    iface.DBScheduler
	SchedulerCfg *SchedulerCfg `toml:"scheduler-cfg"`

	WalRole wal.Role
	Wal     wal.ShardAwareWal

	Meta struct {
		Conf    *MetaCfg
		Catalog *metadata.Catalog
	}

	GC struct {
		Conf     *gci.WorkerCfg
		Acceptor gci.IAcceptor
	}

	CacheCfg *CacheCfg `toml:"cache-cfg"`

	MetaCleanerCfg *MetaCleanerCfg
}

func (o *Options) FillDefaults(dirname string) *Options {
	if o == nil {
		o = &Options{}
	}
	o.EventListener.FillDefaults()

	if o.SchedulerCfg == nil {
		o.SchedulerCfg = &SchedulerCfg{
			BlockWriters:     DefaultBlockWriters,
			SegmentWriters:   DefaultSegmentWriters,
			IndexWriters: DefaultIndexWriters,
			StatelessWorkers: DefaultStatelessWorkers,
		}
	} else {
		if o.SchedulerCfg.BlockWriters == 0 {
			o.SchedulerCfg.BlockWriters = DefaultBlockWriters
		}
		if o.SchedulerCfg.SegmentWriters == 0 {
			o.SchedulerCfg.SegmentWriters = DefaultSegmentWriters
		}
		if o.SchedulerCfg.IndexWriters == 0 {
			o.SchedulerCfg.IndexWriters = DefaultIndexWriters
		}
		if o.SchedulerCfg.StatelessWorkers == 0 {
			o.SchedulerCfg.StatelessWorkers = DefaultStatelessWorkers
		}
	}

	if o.Meta.Conf == nil {
		o.Meta.Conf = &MetaCfg{
			BlockMaxRows:     DefaultBlockMaxRows,
			SegmentMaxBlocks: DefaultBlocksPerSegment,
		}
	}

	if o.CacheCfg == nil {
		o.CacheCfg = &CacheCfg{
			IndexCapacity:  DefaultIndexCacheSize,
			InsertCapacity: DefaultMTCacheSize,
			DataCapacity:   DefaultSSTCacheSize,
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
			Interval: time.Duration(DefaultCleanInterval) * time.Second,
			// Interval: time.Duration(200) * time.Millisecond,
		}
	}
	return o
}

func (o *Options) CreateCatalog(dirname string) (*metadata.Catalog, error) {
	catalog, err := metadata.OpenCatalog(&o.Mu, &metadata.CatalogCfg{
		Dir:              dirname,
		BlockMaxRows:     o.Meta.Conf.BlockMaxRows,
		SegmentMaxBlocks: o.Meta.Conf.SegmentMaxBlocks,
	})
	return catalog, err
}
