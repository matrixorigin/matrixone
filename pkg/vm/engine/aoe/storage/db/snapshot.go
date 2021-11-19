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
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	dbsched "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type SSWriter interface {
	io.Closer
	metadata.SSWriter
	GetIndex() uint64
}

type SSLoader = metadata.SSLoader

type Snapshoter interface {
	SSWriter
	SSLoader
}

type ssWriter struct {
	mwriter metadata.SSWriter
	meta    *metadata.Database
	data    map[uint64]iface.ITableData
	tables  *table.Tables
	dir     string
	index   uint64
}

type ssLoader struct {
	mloader   metadata.SSLoader
	src       string
	database  *metadata.Database
	tables    *table.Tables
	index     uint64
	flushsegs []*metadata.Segment
}

type installContext struct {
	blkfiles     map[common.ID]bool
	sortedsegs   map[common.ID]bool
	unsortedsegs map[common.ID]bool
}

func (ctx *installContext) Preprocess() {
	ctx.unsortedsegs = make(map[common.ID]bool)
	for id, _ := range ctx.blkfiles {
		ctx.unsortedsegs[id.AsSegmentID()] = true
	}
}

func (ctx *installContext) HasBlockFile(id *common.ID) bool {
	_, ok := ctx.blkfiles[*id]
	return ok
}

func (ctx *installContext) HasSegementFile(id *common.ID) bool {
	_, ok := ctx.unsortedsegs[*id]
	if ok {
		return true
	}
	_, ok = ctx.sortedsegs[*id]
	return ok
}

func NewDBSSWriter(database *metadata.Database, dir string, tables *table.Tables) *ssWriter {
	w := &ssWriter{
		data:   make(map[uint64]iface.ITableData),
		tables: tables,
		dir:    dir,
		meta:   database,
	}
	return w
}

func NewDBSSLoader(database *metadata.Database, tables *table.Tables, src string) *ssLoader {
	ss := &ssLoader{
		database:  database,
		src:       src,
		tables:    tables,
		flushsegs: make([]*metadata.Segment, 0),
	}
	return ss
}

func (ss *ssWriter) Close() error {
	for _, t := range ss.data {
		t.Unref()
	}
	return nil
}

func (ss *ssWriter) onTableData(data iface.ITableData) error {
	meta := data.GetMeta()
	if meta.Database == ss.meta {
		data.Ref()
		ss.data[data.GetID()] = data
	}
	return nil
}

func (ss *ssWriter) validateAndRewrite() error {
	// FIXME:
	// 1. Validate the metadata
	// 2. Rewrite the metadata base on copied data files: unsorted -> sorted .etc
	return nil
}

func (ss *ssWriter) GetIndex() uint64 {
	return ss.index
}

func (ss *ssWriter) PrepareWrite() error {
	ss.tables.RLock()
	ss.index = ss.meta.GetCheckpointId()
	err := ss.tables.ForTablesLocked(ss.onTableData)
	ss.tables.RUnlock()
	if err != nil {
		return err
	}
	ss.mwriter = metadata.NewDBSSWriter(ss.meta, ss.dir, ss.index)
	if err = ss.mwriter.PrepareWrite(); err != nil {
		return err
	}
	return ss.validateAndRewrite()
}

func (ss *ssWriter) CommitWrite() error {
	err := ss.mwriter.CommitWrite()
	if err != nil {
		return err
	}
	for _, t := range ss.data {
		if err = CopyTableFn(t, ss.dir); err != nil {
			break
		}
	}
	return err
}

func (ss *ssLoader) validateMetaLoader() error {
	// if ss.index != ss.mloader.GetIndex() {
	// 	return errors.New(fmt.Sprintf("index mismatch: %d, %d", ss.index, ss.mloader.GetIndex()))
	// }
	if ss.database.GetShardId() != ss.mloader.GetShardId() {
		return errors.New(fmt.Sprintf("shard id mismatch: %d, %d", ss.database.ShardId, ss.mloader.GetShardId()))
	}
	return nil
}

func (ss *ssLoader) Preprocess(processor metadata.Processor) error {
	return ss.mloader.Preprocess(processor)
}

func (ss *ssLoader) PrepareLoad() error {
	metas, tblks, blks, segs, err := ScanMigrationDir(ss.src)
	if err != nil {
		return err
	}
	if len(metas) != 1 {
		return errors.New("invalid meta data to apply")
	}

	ss.mloader = metadata.NewDBSSLoader(ss.database.Catalog, filepath.Join(ss.src, metas[0]))
	err = ss.mloader.PrepareLoad()
	if err != nil {
		return err
	}
	if err = ss.validateMetaLoader(); err != nil {
		return err
	}

	destDir := ss.database.Catalog.Cfg.Dir
	blkFiles := make(map[common.ID]bool)
	segFiles := make(map[common.ID]bool)
	blkMapFn := func(id *common.ID) (*common.ID, error) {
		blkFiles[*id] = true
		return ss.mloader.Addresses().GetBlkAddr(id)
	}
	segMapFn := func(id *common.ID) (*common.ID, error) {
		segFiles[*id] = true
		return ss.mloader.Addresses().GetSegAddr(id)
	}
	if err = CopyDataFiles(tblks, blks, segs, ss.src, destDir, blkMapFn, segMapFn); err != nil {
		return err
	}

	// var tableData iface.ITableData
	// var segData iface.ISegment
	// tables := make([]*metadata.Table, 0)
	processor := new(metadata.LoopProcessor)
	processor.TableFn = func(table *metadata.Table) error {
		var err error
		// tableData, err = ss.tables.RegisterTable(table)
		return err
	}
	processor.SegmentFn = func(segment *metadata.Segment) error {
		if segment.IsUpgradable() {
			ss.flushsegs = append(ss.flushsegs, segment)
		}
		return nil
	}
	processor.BlockFn = func(block *metadata.Block) error {
		return nil
	}
	tables := ss.mloader.GetTables()
	ctx := new(installContext)
	ctx.blkfiles = blkFiles
	ctx.sortedsegs = segFiles
	ctx.Preprocess()
	tablesData, err := ss.tables.PrepareInstallTables(tables, ctx)
	if err != nil {
		return err
	}
	if err = ss.tables.CommitInstallTables(tablesData); err != nil {
		return err
	}

	return ss.Preprocess(processor)
}

func (ss *ssLoader) CommitLoad() error {
	err := ss.mloader.CommitLoad()
	if err != nil {
		return err
	}
	return nil
}

func (ss *ssLoader) ScheduleEvents(d *DB) error {
	for _, meta := range ss.flushsegs {
		table, _ := d.GetTableData(meta.Table)
		defer table.Unref()
		segment := table.StrongRefSegment(meta.Id)
		flushCtx := &dbsched.Context{Opts: d.Opts}
		flushEvent := dbsched.NewFlushSegEvent(flushCtx, segment)
		d.Scheduler.Schedule(flushEvent)
	}
	return nil
}
