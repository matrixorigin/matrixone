package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
)

type ShardSnapshot struct {
	Dir            string
	Name           string
	View           *catalogLogEntry
	ShardId, Index uint64
	Catalog        *Catalog
}

func NewShardSnapshotWriter(catalog *Catalog, dir string, shardId, index uint64) *ShardSnapshot {
	ss := &ShardSnapshot{
		Catalog: catalog,
		Dir:     dir,
		ShardId: shardId,
		Index:   index,
	}
	return ss
}

func NewShardSnapshotReader(catalog *Catalog, name string) *ShardSnapshot {
	ss := &ShardSnapshot{
		Catalog: catalog,
		Name:    name,
	}
	return ss
}

func (ss *ShardSnapshot) PrepareWrite() error {
	ss.View = ss.Catalog.ShardView(ss.ShardId, ss.Index)
	return nil
}

func (ss *ShardSnapshot) makeName() string {
	return filepath.Join(ss.Dir, fmt.Sprintf("%d-%d-%d.meta", ss.ShardId, ss.Index, time.Now().UnixMicro()))
}

func (ss *ShardSnapshot) CommitWrite() error {
	f, err := os.Create(ss.makeName())
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := ss.View.Marshal()
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	return err
}

func (ss *ShardSnapshot) PrepareRead() error {
	f, err := os.OpenFile(ss.Name, os.O_RDONLY, 666)
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
	ss.View = &catalogLogEntry{}
	if _, err = f.Read(mnode.Buf[:size]); err != nil {
		return err
	}
	if err = ss.View.Unmarshal(mnode.Buf[:size]); err != nil {
		return err
	}
	processor := newReAllocIdProcessor(&ss.Catalog.Sequence)
	err = ss.View.Catalog.RecurLoop(processor)
	return err
}

func (ss *ShardSnapshot) Apply() error {
	return ss.Catalog.SimpleReplayNewShard(ss.View)
}

type shardLogEntry struct {
	loopProcessor `json:"-"`
	commitId      uint64 `json:"-"`
	OldShard      []*replaceTableCtx
	NewShard      []*addTableCtx
}

func newShardLogEntry(olds []*replaceTableCtx, news []*addTableCtx) *shardLogEntry {
	e := &shardLogEntry{
		OldShard: olds,
		NewShard: news,
	}
	e.TableFn = e.onTable
	e.SegmentFn = e.onSegment
	e.BlockFn = e.onBlock
	return e
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
	for _, ctx := range e.OldShard {
		ctx.table.Lock()
		ctx.table.CommitLocked(commitId)
		ctx.table.Unlock()
	}
	e.commitId = commitId
	for _, ctx := range e.NewShard {
		ctx.table.RLock()
		ctx.table.RecurLoopLocked(e)
		ctx.table.RUnlock()
		ctx.table.Lock()
		ctx.table.CommitLocked(commitId)
		ctx.table.Unlock()
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
