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

package metadata

import (
	"encoding/json"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
)

type TxnEntry LogEntry

type TxnStore struct {
	sync.Mutex `json:"-"`
	Logs       [][]byte `json:"logs"`
	entries    []IEntry
	eTypes     []LogEntryType
}

func (store *TxnStore) Marshal() ([]byte, error) {
	store.Logs = make([][]byte, len(store.entries))
	for i, entry := range store.entries {
		logEntry := entry.ToLogEntry(store.eTypes[i])
		store.Logs[i], _ = logEntry.Marshal()
	}
	return json.Marshal(store)
}

func (store *TxnStore) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, store)
}

func (store *TxnStore) CommitLocked(commitId uint64) {
	for _, entry := range store.entries {
		entry.Lock()
		entry.CommitLocked(commitId)
		entry.Unlock()
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
	err := logEntry.Unmarshal(buf)
	if err != nil {
		panic(err)
	}
	return logEntry
}

func (store *TxnStore) AddEntry(entry IEntry, eType LogEntryType) {
	store.Lock()
	defer store.Unlock()
	store.entries = append(store.entries, entry)
	store.eTypes = append(store.eTypes, eType)
}

func NewTxnStore() *TxnStore {
	return &TxnStore{
		entries: make([]IEntry, 0),
		eTypes:  make([]LogEntryType, 0),
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
