package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type SSWriter interface {
	PrepareWrite() error
	CommitWrite() error
}

type SSLoader interface {
	PrepareLoad() error
	CommitLoad() error
}

type Snapshoter interface {
	SSWriter
	SSLoader
}

type mapping struct {
	Database map[uint64]uint64
	Table    map[uint64]uint64
	Segment  map[uint64]uint64
	Block    map[uint64]uint64
}

func (m *mapping) RewriteSegmentFile(filename string) (string, error) {
	arr := strings.Split(strings.TrimSuffix(filename, ".seg"), "_")
	tableId, err := strconv.Atoi(arr[0])
	if err != nil {
		return "", err
	}
	arr[0] = strconv.Itoa(int(m.Table[uint64(tableId)]))
	segId, err := strconv.Atoi(arr[1])
	if err != nil {
		return "", err
	}
	arr[1] = strconv.Itoa(int(m.Segment[uint64(segId)]))
	return arr[0] + "_" + arr[1] + ".seg", nil
}

func (m *mapping) RewriteBlockFile(filename string) (string, error) {
	arr := strings.Split(strings.TrimSuffix(filename, ".blk"), "_")
	tableId, err := strconv.Atoi(arr[0])
	if err != nil {
		return "", err
	}
	arr[0] = strconv.Itoa(int(m.Table[uint64(tableId)]))
	segId, err := strconv.Atoi(arr[1])
	if err != nil {
		return "", err
	}
	arr[1] = strconv.Itoa(int(m.Segment[uint64(segId)]))
	blkId, err := strconv.Atoi(arr[2])
	if err != nil {
		return "", err
	}
	arr[2] = strconv.Itoa(int(m.Block[uint64(blkId)]))
	return arr[0] + "_" + arr[1] + "_" + arr[2] + ".blk", nil
}

type dbSnapshoter struct {
	dir     string
	name    string
	view    *databaseLogEntry
	index   uint64
	tranId  uint64
	db      *Database
	catalog *Catalog
	mapping *mapping
}

func NewDBSSWriter(db *Database, dir string, index uint64) *dbSnapshoter {
	ss := &dbSnapshoter{
		db:    db,
		dir:   dir,
		index: index,
	}
	return ss
}

func NewDBSSLoader(catalog *Catalog, name string) *dbSnapshoter {
	ss := &dbSnapshoter{
		catalog: catalog,
		name:    name,
	}
	return ss
}

func (ss *dbSnapshoter) PrepareWrite() error {
	ss.view = ss.db.View(ss.index)
	ss.view.Id = ss.db.Id
	return nil
}

func (ss *dbSnapshoter) makeName() string {
	return filepath.Join(ss.dir, fmt.Sprintf("%d-%d-%d.meta", ss.db.GetShardId(), ss.index, time.Now().UTC().UnixMicro()))
}

func (ss *dbSnapshoter) CommitWrite() error {
	ss.name = ss.makeName()
	f, err := os.Create(ss.name)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := ss.view.Marshal()
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	logutil.Infof("%s | Shard SS | Flushed", f.Name())
	return err
}

func (ss *dbSnapshoter) ReAllocId(allocator *Sequence, view *Database) error {
	ss.tranId = allocator.NextUncommitId()
	processor := newReAllocIdProcessor(allocator, ss.tranId)
	ss.mapping = processor.trace
	processor.OnDatabase(view)
	err := view.RecurLoopLocked(processor)
	if err != nil {
		return err
	}
	tableSet := make(map[uint64]*Table)
	for _, table := range ss.view.Database.TableSet {
		tableSet[table.Id] = table
	}
	ss.view.Database.TableSet = tableSet
	return err
}

func (ss *dbSnapshoter) PrepareLoad() error {
	f, err := os.OpenFile(ss.name, os.O_RDONLY, 666)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	mnode := common.GPool.Alloc(uint64(size))
	defer common.GPool.Free(mnode)
	ss.view = &databaseLogEntry{}
	if _, err = f.Read(mnode.Buf[:size]); err != nil {
		return err
	}
	if err = ss.view.Unmarshal(mnode.Buf[:size]); err != nil {
		return err
	}
	ss.view.Database.Catalog = ss.catalog
	ss.view.Database.rebuild(true, false)
	return ss.ReAllocId(&ss.catalog.Sequence, ss.view.Database)
}

func (ss *dbSnapshoter) CommitLoad() error {
	return ss.catalog.SimpleReplaceDatabase(ss.view, ss.tranId)
}

func (ss *dbSnapshoter) View() *databaseLogEntry {
	return ss.view
}

func (ss *dbSnapshoter) Mapping() *mapping {
	return ss.mapping
}
