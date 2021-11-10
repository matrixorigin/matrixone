package metadata

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
)

type databaseLogEntry struct {
	*BaseEntry
	Id       uint64    `json:"oid"`
	Database *Database `json:"db"`
	LogRange *LogRange `json:"logrange"`
}

type dbReplaceLogEntry struct {
	loopProcessor `json:"-"`
	commitId      uint64            `json:"-"`
	Replaced      *databaseLogEntry `json:"replaced"`
	Replacer      []*Database       `json:"replacer"`
}

type catalogLogEntry struct {
	Range    *common.Range `json:"range"`
	Catalog  *Catalog      `json:"catalog"`
	LogRange *LogRange     `json:"logrange"`
}

func newDatabaseLogEntry(shardId, index uint64) *databaseLogEntry {
	logRange := new(LogRange)
	logRange.ShardId = shardId
	logRange.Range.Right = index
	e := &databaseLogEntry{
		Database: &Database{
			TableSet: make(map[uint64]*Table),
		},
		LogRange: logRange,
	}
	return e
}

func (e *databaseLogEntry) Marshal() ([]byte, error) {
	buf, err := json.Marshal(e)
	return buf, err
}

func (e *databaseLogEntry) Unmarshal(buf []byte) error {
	err := json.Unmarshal(buf, e)
	if err != nil {
		return err
	}
	if e.Database != nil {
		e.Database.coarseStats = new(coarseStats)
	}
	return nil
}

func (e *databaseLogEntry) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case ETDatabaseSnapshot:
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

func newDbReplaceLogEntry() *dbReplaceLogEntry {
	e := &dbReplaceLogEntry{
		Replacer: make([]*Database, 0),
	}
	e.TableFn = e.onTable
	e.SegmentFn = e.onSegment
	e.BlockFn = e.onBlock
	return e
}

func (e *dbReplaceLogEntry) AddReplacer(db *Database) {
	e.Replacer = append(e.Replacer, db)
}

func (e *dbReplaceLogEntry) onTable(table *Table) error {
	table.Lock()
	defer table.Unlock()
	table.CommitLocked(e.commitId)
	return nil
}

func (e *dbReplaceLogEntry) onSegment(segment *Segment) error {
	segment.Lock()
	defer segment.Unlock()
	segment.CommitLocked(e.commitId)
	return nil
}

func (e *dbReplaceLogEntry) onBlock(block *Block) error {
	block.Lock()
	defer block.Unlock()
	block.CommitLocked(e.commitId)
	return nil
}

func (e *dbReplaceLogEntry) Lock()   {}
func (e *dbReplaceLogEntry) Unlock() {}

func (e *dbReplaceLogEntry) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *dbReplaceLogEntry) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

func (e *dbReplaceLogEntry) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case ETReplaceDatabase:
	case ETSplitDatabase:
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

func (e *dbReplaceLogEntry) CommitLocked(commitId uint64) {
	e.Replaced.Lock()
	e.Replaced.CommitLocked(commitId)
	e.Replaced.Unlock()
	e.commitId = commitId
	for _, db := range e.Replacer {
		db.RLock()
		db.RecurLoopLocked(e)
		db.RUnlock()
		db.Lock()
		db.CommitLocked(commitId)
		db.Unlock()
	}
}
