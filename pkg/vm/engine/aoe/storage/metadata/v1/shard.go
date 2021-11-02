package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
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

type shardSnapshoter struct {
	dir            string
	name           string
	view           *catalogLogEntry
	shardId, index uint64
	catalog        *Catalog
}

func NewShardSSWriter(catalog *Catalog, dir string, shardId, index uint64) *shardSnapshoter {
	ss := &shardSnapshoter{
		catalog: catalog,
		dir:     dir,
		shardId: shardId,
		index:   index,
	}
	return ss
}

func NewShardSSLoader(catalog *Catalog, name string) *shardSnapshoter {
	ss := &shardSnapshoter{
		catalog: catalog,
		name:    name,
	}
	return ss
}

func (ss *shardSnapshoter) PrepareWrite() error {
	ss.view = ss.catalog.ShardView(ss.shardId, ss.index)
	return nil
}

func (ss *shardSnapshoter) makeName() string {
	return filepath.Join(ss.dir, fmt.Sprintf("%d-%d-%d.meta", ss.shardId, ss.index, time.Now().UTC().UnixMicro()))
}

func (ss *shardSnapshoter) CommitWrite() error {
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

func (ss *shardSnapshoter) ReAllocId(alloctor *Sequence, view *Catalog) error {
	processor := newReAllocIdProcessor(alloctor)
	return view.RecurLoop(processor)
}

func (ss *shardSnapshoter) PrepareLoad() error {
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
	ss.view = &catalogLogEntry{}
	if _, err = f.Read(mnode.Buf[:size]); err != nil {
		return err
	}
	if err = ss.view.Unmarshal(mnode.Buf[:size]); err != nil {
		return err
	}
	return ss.ReAllocId(&ss.catalog.Sequence, ss.view.Catalog)
}

func (ss *shardSnapshoter) CommitLoad() error {
	return ss.catalog.SimpleReplaceShard(ss.view)
}

type shardLogEntry struct {
	loopProcessor `json:"-"`
	commitId      uint64   `json:"-"`
	Replaced      []*Table `json:"replaced"`
	Replacer      []*Table `json:"replacer"`
}

func newShardLogEntry() *shardLogEntry {
	e := &shardLogEntry{
		Replaced: make([]*Table, 0),
		Replacer: make([]*Table, 0),
	}
	e.TableFn = e.onTable
	e.SegmentFn = e.onSegment
	e.BlockFn = e.onBlock
	return e
}

func (e *shardLogEntry) addReplaced(table *Table) {
	e.Replaced = append(e.Replaced, table)
}

func (e *shardLogEntry) addReplacer(table *Table) {
	e.Replacer = append(e.Replacer, table)
}

func (e *shardLogEntry) onTable(table *Table) error {
	table.Lock()
	defer table.Unlock()
	table.CommitLocked(e.commitId)
	return nil
}

func (e *shardLogEntry) onSegment(segment *Segment) error {
	segment.Lock()
	defer segment.Unlock()
	segment.CommitLocked(e.commitId)
	return nil
}

func (e *shardLogEntry) onBlock(block *Block) error {
	block.Lock()
	defer block.Unlock()
	block.CommitLocked(e.commitId)
	return nil
}

func (e *shardLogEntry) Lock()   {}
func (e *shardLogEntry) Unlock() {}

func (e *shardLogEntry) CommitLocked(commitId uint64) {
	for _, table := range e.Replaced {
		table.Lock()
		table.CommitLocked(commitId)
		table.Unlock()
	}
	e.commitId = commitId
	for _, table := range e.Replacer {
		table.RLock()
		table.RecurLoopLocked(e)
		table.RUnlock()
		table.Lock()
		table.CommitLocked(commitId)
		table.Unlock()
	}
}

func (e *shardLogEntry) Marshal() ([]byte, error) {
	buf, err := json.Marshal(e)
	return buf, err
}

func (e *shardLogEntry) Unmarshal(buf []byte) error {
	if err := json.Unmarshal(buf, e); err != nil {
		return err
	}
	return nil
}

func (e *shardLogEntry) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case ETShardSnapshot:
		break
	default:
		panic("not supported")
	}
	buf, _ := e.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

type shardStats struct {
	id          uint64
	coarseSize  int64
	coarseCount uint64
	// updated     time.Time
}

func newShardStats(id uint64) *shardStats {
	return &shardStats{
		id: id,
	}
}

func (stats *shardStats) addCount(count uint64) {
	atomic.AddUint64(&stats.coarseCount, count)
}

func (stats *shardStats) getCount() uint64 {
	return atomic.LoadUint64(&stats.coarseCount)
}

func (stats *shardStats) addSize(size int64) {
	atomic.AddInt64(&stats.coarseSize, size)
}

func (stats *shardStats) getSize() int64 {
	return atomic.LoadInt64(&stats.coarseSize)
}

func (stats *shardStats) String() string {
	return fmt.Sprintf("Stats<%d>(Size=%d, Count=%d)", stats.id, stats.getSize(), stats.getCount())
}
