package metadata

import (
	"encoding/json"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
)

type TxnEntry struct {
	typ     LogEntryType
	payload IEntry
}

type TxnStore struct {
	sync.Mutex `json:"-"`
	Entries    []*TxnEntry `json:"entries"`
}

func (store *TxnStore) Marshal() ([]byte, error) {
	return json.Marshal(store)
}

func (store *TxnStore) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, store)
}

func (store *TxnStore) CommitLocked(commitId uint64) {
	for _, entry := range store.Entries {
		entry.payload.Lock()
		entry.payload.CommitLocked(commitId)
		entry.payload.Unlock()
	}
}

func (store *TxnStore) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case ETTransaction:
		break
	default:
		panic("not supported")
	}
	buf, _ := store.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	logEntry.Unmarshal(buf)
	return logEntry
}

func (store *TxnStore) AddEntry(entry IEntry, eType LogEntryType) {
	txnEntry := &TxnEntry{
		payload: entry,
		typ:     eType,
	}
	store.Entries = append(store.Entries, txnEntry)
}

func NewTxnStore() *TxnStore {
	return &TxnStore{
		Entries: make([]*TxnEntry, 0),
	}
}

type TxnCtx struct {
	catalog  *Catalog
	tranId   uint64
	commitId uint64
	store    *TxnStore
	index    *LogIndex
}

func NewTxn(catalog *Catalog, index *LogIndex) *TxnCtx {
	txn := &TxnCtx{
		catalog: catalog,
		tranId:  catalog.NextUncommitId(),
		store:   NewTxnStore(),
		index:   index,
	}
	txn.commitId = txn.tranId
	return txn
}

func (txn *TxnCtx) AddEntry(entry IEntry, eType LogEntryType) {
	txn.store.AddEntry(entry, eType)
}

func (txn *TxnCtx) Commit() error {
	return txn.catalog.CommitTxn(txn)
}

func (txn *TxnCtx) Abort() error {
	return txn.catalog.AbortTxn(txn)
}
