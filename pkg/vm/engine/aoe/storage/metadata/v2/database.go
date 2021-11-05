package metadata

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
)

type Database struct {
	*BaseEntry
	nodesMu   sync.RWMutex          `json:"-"`
	nameNodes map[string]*tableNode `json:"-"`
	Catalog   *Catalog              `json:"-"`
	Name      string                `json:"name"`

	TableSet map[uint64]*Table `json:"tables"`
}

func (db *Database) SimpleHardDelete() error {
	tranId := db.Catalog.NextUncommitId()
	ctx := new(deleteDatabaseCtx)
	ctx.tranId = tranId
	ctx.database = db
	err := db.Catalog.onCommitRequest(ctx)
	return err
}

func (db *Database) prepareHardDelete(ctx *deleteDatabaseCtx) (LogEntry, error) {
	cInfo := &CommitInfo{
		CommitId: ctx.tranId,
		TranId:   ctx.tranId,
		Op:       OpHardDelete,
		SSLLNode: *common.NewSSLLNode(),
	}
	db.Lock()
	defer db.Unlock()
	if db.IsHardDeletedLocked() {
		logutil.Warnf("HardDelete %d but already hard deleted", db.Id)
		return nil, TableNotFoundErr
	}
	if !db.IsSoftDeletedLocked() && !db.IsReplacedLocked() {
		panic("logic error: Cannot hard delete entry that not soft deleted or replaced")
	}
	cInfo.LogIndex = db.CommitInfo.LogIndex
	db.onNewCommit(cInfo)
	logEntry := db.Catalog.prepareCommitEntry(db, ETHardDeleteTable, db)
	return logEntry, nil
}

func (db *Database) SimpleSoftDelete() error {
	tranId := db.Catalog.NextUncommitId()
	ctx := new(dropDatabaseCtx)
	ctx.tranId = tranId
	ctx.database = db
	return db.Catalog.onCommitRequest(ctx)
}

func (db *Database) prepareSoftDelete(ctx *dropDatabaseCtx) (LogEntry, error) {
	cInfo := &CommitInfo{
		TranId:   ctx.tranId,
		CommitId: ctx.tranId,
		LogIndex: ctx.exIndex,
		Op:       OpSoftDelete,
		SSLLNode: *common.NewSSLLNode(),
	}
	db.Lock()
	defer db.Unlock()
	if db.IsSoftDeletedLocked() {
		return nil, TableNotFoundErr
	}
	db.onNewCommit(cInfo)
	logEntry := db.Catalog.prepareCommitEntry(db, ETSoftDeleteDatabase, db)
	return logEntry, nil
}

func (db *Database) Marshal() ([]byte, error) {
	return json.Marshal(db)
}

func (db *Database) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, db)
}

func (db *Database) String() string {
	buf, _ := db.Marshal()
	return string(buf)
}

// Not safe
// Usually it is used during creating a table. We need to commit the new table entry
// to the store.
func (db *Database) ToLogEntry(eType LogEntryType) LogEntry {
	var buf []byte
	switch eType {
	case ETCreateTable:
		buf, _ = db.Marshal()
	case ETSoftDeleteTable:
		if !db.IsSoftDeletedLocked() {
			panic("logic error")
		}
		entry := tableLogEntry{
			BaseEntry: db.BaseEntry,
		}
		buf, _ = entry.Marshal()
	case ETHardDeleteTable:
		if !db.IsHardDeletedLocked() {
			panic("logic error")
		}
		entry := tableLogEntry{
			BaseEntry: db.BaseEntry,
		}
		buf, _ = entry.Marshal()
	default:
		panic(fmt.Sprintf("not supported: %d", eType))
	}
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

func (db *Database) PString(level PPLevel) string {
	db.RLock()
	s := fmt.Sprintf("<Database[%d][%s]>(Cnt=%d){", db.Id, db.Name, len(db.TableSet))
	for _, table := range db.TableSet {
		s = fmt.Sprintf("%s\n%s", s, table.PString(level))
	}
	if len(db.TableSet) == 0 {
		s = fmt.Sprintf("%s}", s)
	} else {
		s = fmt.Sprintf("%s\n}", s)
	}
	db.RUnlock()
	return s
}
