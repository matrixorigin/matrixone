package metadata

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
)

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
	case ETShardSplit:
	default:
		panic("not supported")
	}
	buf, _ := e.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}
