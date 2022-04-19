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
	"io/ioutil"
	"os"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/event"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type Splitter struct {
	msplitter   *metadata.ShardSplitter
	database    *metadata.Database
	renameTable RenameTableFactory
	ctx         []byte
	keys        [][]byte
	index       *LogIndex
	dbSpecs     []*metadata.DBSpec
	dbImpl      *DB
	tables      map[uint64]iface.ITableData
	tempDir     string
	flushsegs   []*metadata.Segment
	event       *event.SplitEvent
}

func NewSplitter(database *metadata.Database, newDBNames []string, rename RenameTableFactory,
	keys [][]byte, ctx []byte, index *LogIndex, dbImpl *DB) *Splitter {
	splitter := &Splitter{
		database:    database,
		renameTable: rename,
		ctx:         ctx,
		keys:        keys,
		index:       index,
		dbImpl:      dbImpl,
		tables:      make(map[uint64]iface.ITableData),
		flushsegs:   make([]*metadata.Segment, 0),
	}
	splitter.dbSpecs = make([]*metadata.DBSpec, len(splitter.keys))
	for i := range splitter.dbSpecs {
		dbSpec := new(metadata.DBSpec)
		dbSpec.Name = newDBNames[i]
		splitter.dbSpecs[i] = dbSpec
	}
	return splitter
}

func (splitter *Splitter) onTableData(data iface.ITableData) error {
	meta := data.GetMeta()
	if meta.Database == splitter.database {
		data.Ref()
		splitter.tables[data.GetID()] = data
	}
	return nil
}

func (splitter *Splitter) prepareData() error {
	err := splitter.dbImpl.Store.DataTables.ForTables(splitter.onTableData)
	if err != nil {
		return err
	}
	if splitter.tempDir, err = ioutil.TempDir(splitter.dbImpl.GetTempDir(), "aoesplit"); err != nil {
		return err
	}
	for _, t := range splitter.tables {
		if err = CopyTableFn(t, splitter.tempDir); err != nil {
			return err
		}
	}
	_, tblks, blks, segs, err := ScanMigrationDir(splitter.tempDir)
	if err != nil {
		return err
	}

	destDir := splitter.database.Catalog.Cfg.Dir
	blkFiles := make(map[common.ID]bool)
	segFiles := make(map[common.ID]bool)
	blkMapFn := func(id *common.ID) (*common.ID, error) {
		nid, err := splitter.msplitter.Spec.GetBlkAddr(id)
		if err != nil {
			return nil, err
		}
		blkFiles[*nid] = true
		return nid, nil
	}
	segMapFn := func(id *common.ID) (*common.ID, error) {
		nid, err := splitter.msplitter.Spec.GetSegAddr(id)
		if err != nil {
			return nil, err
		}
		segFiles[*nid] = true
		return nid, nil
	}

	if err = CopyDataFiles(tblks, blks, segs, splitter.tempDir, destDir, blkMapFn, segMapFn); err != nil {
		return err
	}

	processor := new(metadata.LoopProcessor)
	processor.SegmentFn = func(segment *metadata.Segment) error {
		if segment.IsUpgradable() {
			id := segment.AsCommonID()
			_, found := segFiles[*id]
			if !found {
				splitter.flushsegs = append(splitter.flushsegs, segment)
			} else {
				name := common.MakeSegmentFileName(destDir, id.ToSegmentFileName(), id.TableID, false)
				info, err := os.Stat(name)
				if err != nil {
					return err
				}
				segment.DryUpgrade(info.Size())
			}
		}
		return nil
	}
	splitter.event = new(event.SplitEvent)
	splitter.event.DB = splitter.database.Name
	splitter.event.Names = make(map[string][]string)
	processor.TableFn = func(table *metadata.Table) error {
		splitter.event.Names[table.Database.Name] = append(splitter.event.Names[table.Database.Name], table.Schema.Name)
		return nil
	}
	if err = splitter.msplitter.Preprocess(processor); err != nil {
		return err
	}
	ctx := new(installContext)
	ctx.blkfiles = blkFiles
	ctx.sortedsegs = segFiles
	ctx.Preprocess()
	for _, database := range splitter.msplitter.ReplaceCtx.Replacer {
		tablesData, err := splitter.dbImpl.Store.DataTables.PrepareInstallTables(database.TableSet, ctx)
		if err != nil {
			return err
		}
		if err = splitter.dbImpl.Store.DataTables.CommitInstallTables(tablesData); err != nil {
			return err
		}
	}

	return err
}

func (splitter *Splitter) Prepare() error {
	var err error
	spec := metadata.NewEmptyShardSplitSpec()
	if err = spec.Unmarshal(splitter.ctx); err != nil {
		return err
	}
	spec.Index = splitter.index.Id.Id
	logutil.Infof("PrepareExec Split Spec: %s", spec.String())
	if err = splitter.dbImpl.DoFlushDatabase(splitter.database); err != nil {
		return err
	}
	splitter.msplitter = metadata.NewShardSplitter(splitter.database.Catalog, spec, splitter.dbSpecs, splitter.index, splitter.renameTable)
	if err = splitter.msplitter.Prepare(); err != nil {
		return err
	}
	if err = splitter.prepareData(); err != nil {
		return err
	}
	return err
}

func (splitter *Splitter) Commit() error {
	err := splitter.dbImpl.Opts.EventListener.OnPreSplit(splitter.event)
	if err != nil {
		panic(err)
	}
	err = splitter.msplitter.Commit()
	splitter.dbImpl.Opts.EventListener.OnPostSplit(err, splitter.event)
	return err
}

func (splitter *Splitter) Close() error {
	for _, t := range splitter.tables {
		t.Unref()
	}
	if splitter.tempDir != "" {
		os.RemoveAll(splitter.tempDir)
	}
	return nil
}

func (splitter *Splitter) ScheduleEvents(d *DB) error {
	for _, meta := range splitter.flushsegs {
		table, _ := d.GetTableData(meta.Table)
		defer table.Unref()
		segment := table.StrongRefSegment(meta.Id)
		flushCtx := &sched.Context{Opts: d.Opts}
		flushEvent := sched.NewFlushSegEvent(flushCtx, segment)
		d.Scheduler.Schedule(flushEvent)
	}
	return nil
}
