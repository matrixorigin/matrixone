package db

import (
	"errors"
	"fmt"
	"io"
	"matrixone/pkg/vm/engine/aoe"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/events/meta"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/handle"
	tiface "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mdops "matrixone/pkg/vm/engine/aoe/storage/ops/memdata/v2"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"os"
	"sync"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

var (
	ErrClosed      = errors.New("aoe: closed")
	ErrUnsupported = errors.New("aoe: unsupported")
	ErrNotFound    = errors.New("aoe: notfound")
)

type DB struct {
	Dir  string
	Opts *e.Options

	FsMgr       base.IManager
	MemTableMgr mtif.IManager

	IndexBufMgr bmgrif.IBufferManager
	MTBufMgr    bmgrif.IBufferManager
	SSTBufMgr   bmgrif.IBufferManager

	Store struct {
		Mu         *sync.RWMutex
		MetaInfo   *md.MetaInfo
		DataTables *table.Tables
	}

	Cleaner struct {
		MetaFiles iw.IHeartbeater
	}

	DataDir  *os.File
	DBLocker io.Closer

	Scheduler sched.Scheduler

	Closed  *atomic.Value
	ClosedC chan struct{}
}

func (d *DB) Append(ctx dbi.AppendCtx) (err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	tbl, err := d.Store.MetaInfo.ReferenceTableByName(ctx.TableName)
	if err != nil {
		return err
	}

	collection := d.MemTableMgr.StrongRefCollection(tbl.GetID())
	if collection == nil {
		opCtx := &mdops.OpCtx{
			Opts:        d.Opts,
			MTManager:   d.MemTableMgr,
			TableMeta:   tbl,
			IndexBufMgr: d.IndexBufMgr,
			MTBufMgr:    d.MTBufMgr,
			SSTBufMgr:   d.SSTBufMgr,
			FsMgr:       d.FsMgr,
			Tables:      d.Store.DataTables,
		}
		op := mdops.NewCreateTableOp(opCtx)
		op.Push()
		err = op.WaitDone()
		if err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		collection = op.Collection
	}

	index := &md.LogIndex{
		ID:       ctx.OpIndex,
		Capacity: uint64(ctx.Data.Vecs[0].Length()),
	}
	defer collection.Unref()
	return collection.Append(ctx.Data, index)
}

func (d *DB) getTableData(meta *md.Table) (tiface.ITableData, error) {
	data, err := d.Store.DataTables.StrongRefTable(meta.ID)
	if err != nil {
		opCtx := &mdops.OpCtx{
			Opts:        d.Opts,
			MTManager:   d.MemTableMgr,
			TableMeta:   meta,
			IndexBufMgr: d.IndexBufMgr,
			MTBufMgr:    d.MTBufMgr,
			SSTBufMgr:   d.SSTBufMgr,
			FsMgr:       d.FsMgr,
			Tables:      d.Store.DataTables,
		}
		op := mdops.NewCreateTableOp(opCtx)
		op.Push()
		err = op.WaitDone()
		if err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		collection := op.Collection
		if data, err = d.Store.DataTables.StrongRefTable(meta.ID); err != nil {
			collection.Unref()
			return nil, err
		}
		collection.Unref()
	}
	return data, nil
}

func (d *DB) Relation(name string) (*Relation, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	meta, err := d.Opts.Meta.Info.ReferenceTableByName(name)
	if err != nil {
		return nil, err
	}
	data, err := d.getTableData(meta)
	if err != nil {
		return nil, err
	}
	return NewRelation(d, data, meta), nil
}

func (d *DB) HasTable(name string) bool {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	_, err := d.Store.MetaInfo.ReferenceTableByName(name)
	return err == nil
}

func (d *DB) DropTable(ctx dbi.DropTableCtx) (id uint64, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	eCtx := &meta.Context{
		Opts: d.Opts,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	e := meta.NewDropTableEvent(eCtx, ctx, d.MemTableMgr, d.Store.DataTables, func() {
		wg.Done()
	})
	if err = d.Scheduler.Schedule(e); err != nil {
		wg.Done()
		return id, err
	}
	wg.Wait()
	return e.Id, e.Err
}

func (d *DB) CreateTable(info *aoe.TableInfo, ctx dbi.TableOpCtx) (id uint64, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	info.Name = ctx.TableName

	var wg sync.WaitGroup
	wg.Add(1)
	eCtx := &meta.Context{Opts: d.Opts}
	e := meta.NewCreateTableEvent(eCtx, ctx, info, func() {
		wg.Done()
	})
	if err = d.Opts.Scheduler.Schedule(e); err != nil {
		wg.Done()
		return id, err
	}
	wg.Wait()
	if e.Err != nil {
		return id, e.Err
	}
	id = e.GetTable().GetID()
	return id, nil
}

func (d *DB) GetSegmentIds(ctx dbi.GetSegmentsCtx) (ids IDS) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	meta, err := d.Opts.Meta.Info.ReferenceTableByName(ctx.TableName)
	if err != nil {
		return ids
	}
	data, err := d.getTableData(meta)
	if err != nil {
		return ids
	}
	ids.Ids = data.SegmentIds()
	// for _, id := range ids {
	// 	infos = append(infos, engine.SegmentInfo{Id: strconv.FormatUint(id, 10)})
	// }
	return ids
}

func (d *DB) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	tableMeta, err := d.Store.MetaInfo.ReferenceTableByName(ctx.TableName)
	if err != nil {
		return nil, err
	}
	if tableMeta.GetSegmentCount() == uint64(0) {
		return handle.NewEmptySnapshot(), nil
	}
	tableData, err := d.Store.DataTables.StrongRefTable(tableMeta.ID)
	if err != nil {
		return nil, err
	}
	var ss *handle.Snapshot
	if ctx.ScanAll {
		ss = handle.NewLinkAllSnapshot(ctx.Cols, tableData)
	} else {
		ss = handle.NewSnapshot(ctx.SegmentIds, ctx.Cols, tableData)
	}
	return ss, nil
}

func (d *DB) TableIDs() (ids []uint64, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	tids := d.Store.MetaInfo.TableIDs()
	for tid := range tids {
		ids = append(ids, tid)
	}
	return ids, err
}

func (d *DB) TableNames() []string {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	return d.Opts.Meta.Info.TableNames()
}

func (d *DB) TableSegmentIDs(tableID uint64) (ids []common.ID, err error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	sids, err := d.Store.MetaInfo.TableSegmentIDs(tableID)
	if err != nil {
		return ids, err
	}
	// TODO: Refactor metainfo to 1. keep order 2. use common.ID
	for sid := range sids {
		ids = append(ids, common.ID{TableID: tableID, SegmentID: sid})
	}
	return ids, err
}

func (d *DB) GetSegmentedId(ctx dbi.GetSegmentedIdCtx) (id uint64, err error) {
	id = ^uint64(0)
	for _, matcher := range ctx.Matchers {
		switch matcher.Type {
		case dbi.MTPrefix:
			tbls := d.Store.MetaInfo.GetTablesByNamePrefix(matcher.Pattern)
			for _, tbl := range tbls {
				data, err := d.getTableData(tbl)
				if err != nil {
					return id, err
				}
				tmpId, ok := data.GetSegmentedIndex()
				if !ok {
					return 0, nil
				}
				if tmpId < id {
					id = tmpId
				}
			}
		default:
			panic("not supported")
		}
	}
	if id == ^uint64(0) {
		return id, ErrNotFound
	}
	return id, err
}

func (d *DB) replayAndCleanData() {
	err := d.Store.DataTables.Replay(d.FsMgr, d.IndexBufMgr, d.MTBufMgr, d.SSTBufMgr, d.Store.MetaInfo)
	if err != nil {
		panic(err)
	}
}

func (d *DB) startCleaner() {
	d.Cleaner.MetaFiles.Start()
}

func (d *DB) startWorkers() {
	d.Opts.GC.Acceptor.Start()
	d.Opts.MemData.Updater.Start()
	d.Opts.Data.Flusher.Start()
	d.Opts.Data.Sorter.Start()
	d.Opts.Meta.Flusher.Start()
	d.Opts.Meta.Updater.Start()
}

func (d *DB) EnsureNotClosed() {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
}

func (d *DB) IsClosed() bool {
	if err := d.Closed.Load(); err != nil {
		return true
	}
	return false
}

func (d *DB) stopWorkers() {
	d.Opts.GC.Acceptor.Stop()
	d.Opts.MemData.Updater.Stop()
	d.Opts.Data.Flusher.Stop()
	d.Opts.Data.Sorter.Stop()
	d.Opts.Meta.Flusher.Stop()
	d.Opts.Meta.Updater.Stop()
}

func (d *DB) stopCleaner() {
	d.Cleaner.MetaFiles.Stop()
}

func (d *DB) WorkersStatsString() string {
	s := fmt.Sprintf("%s\n", d.Opts.MemData.Updater.StatsString())
	s = fmt.Sprintf("%s%s\n", s, d.Opts.Data.Flusher.StatsString())
	s = fmt.Sprintf("%s%s\n", s, d.Opts.Data.Sorter.StatsString())
	s = fmt.Sprintf("%s%s\n", s, d.Opts.Meta.Updater.StatsString())
	s = fmt.Sprintf("%s%s\n", s, d.Opts.Meta.Flusher.StatsString())
	return s
}

func (d *DB) Close() error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}

	d.Closed.Store(ErrClosed)
	close(d.ClosedC)
	d.Scheduler.Stop()
	d.stopWorkers()
	d.stopCleaner()
	err := d.DBLocker.Close()
	return err
}
