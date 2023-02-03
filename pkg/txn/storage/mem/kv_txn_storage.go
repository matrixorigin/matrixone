// Copyright 2021 - 2022 Matrix Origin
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

package mem

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
)

// MustParseGetPayload must parse get payload
func MustParseGetPayload(payload []byte) [][]byte {
	r := &message{}
	r.mustUnmarshal(payload)
	return r.Values
}

// NewSetTxnRequest returns a kv set txn request
func NewSetTxnRequest(ks, vs [][]byte) txn.TxnRequest {
	r := &message{Keys: ks, Values: vs}
	return txn.TxnRequest{
		Method: txn.TxnMethod_Write,
		CNRequest: &txn.CNOpRequest{
			OpCode:  setOpCode,
			Payload: r.mustMarshal(),
		},
	}
}

// NewGetTxnRequest returns a kv get txn request
func NewGetTxnRequest(ks [][]byte) txn.TxnRequest {
	r := &message{Keys: ks}
	return txn.TxnRequest{
		Method: txn.TxnMethod_Read,
		CNRequest: &txn.CNOpRequest{
			OpCode:  getOpCode,
			Payload: r.mustMarshal(),
		},
	}
}

// EventType event type
type EventType int

var (
	// PrepareType prepare event
	PrepareType = EventType(0)
	// CommitType commit event
	CommitType = EventType(1)
	// CommittingType committing type
	CommittingType = EventType(2)
	// RollbackType rollback type
	RollbackType = EventType(3)
)

// Event event
type Event struct {
	// Txn event txn
	Txn txn.TxnMeta
	// Type event type
	Type EventType
}

// KVTxnStorage KV-based implementation of TxnStorage. Just used to test.
type KVTxnStorage struct {
	sync.RWMutex
	logClient            logservice.Client
	clock                clock.Clock
	recoverFrom          logservice.Lsn
	uncommittedTxn       map[string]*txn.TxnMeta
	uncommittedKeyTxnMap map[string]*txn.TxnMeta
	uncommitted          *KV
	committed            *MVCCKV
	eventC               chan Event
}

// NewKVTxnStorage create KV-based implementation of TxnStorage
func NewKVTxnStorage(recoverFrom logservice.Lsn, logClient logservice.Client, clock clock.Clock) *KVTxnStorage {
	return &KVTxnStorage{
		logClient:            logClient,
		clock:                clock,
		recoverFrom:          recoverFrom,
		uncommittedKeyTxnMap: make(map[string]*txn.TxnMeta),
		uncommittedTxn:       make(map[string]*txn.TxnMeta),
		uncommitted:          NewKV(),
		committed:            NewMVCCKV(),
		eventC:               make(chan Event, 1024*10),
	}
}

func (kv *KVTxnStorage) GetEventC() chan Event {
	return kv.eventC
}

func (kv *KVTxnStorage) GetUncommittedTxn(txnID []byte) *txn.TxnMeta {
	kv.RLock()
	defer kv.RUnlock()

	return kv.uncommittedTxn[string(txnID)]
}

func (kv *KVTxnStorage) GetCommittedKV() *MVCCKV {
	return kv.committed
}

func (kv *KVTxnStorage) GetUncommittedKV() *KV {
	return kv.uncommitted
}

func (kv *KVTxnStorage) StartRecovery(ctx context.Context, c chan txn.TxnMeta) {
	defer close(c)

	if kv.recoverFrom < 1 {
		return
	}

	for {
		logs, lsn, err := kv.logClient.Read(ctx, kv.recoverFrom, math.MaxUint64)
		if err != nil {
			panic(err)
		}

		for _, log := range logs {
			if log.Type == logpb.UserRecord {
				klog := &KVLog{}
				klog.MustUnmarshal(log.Data)

				switch klog.Txn.Status {
				case txn.TxnStatus_Prepared:
					req := &message{}
					req.Keys = klog.Keys
					req.Values = klog.Values
					_, err := kv.Write(ctx, klog.Txn, setOpCode, req.mustMarshal())
					if err != nil {
						panic(err)
					}
				case txn.TxnStatus_Committed:
					kv.Lock()
					if len(klog.Keys) == 0 {
						kv.commitKeysLocked(klog.Txn, kv.getWriteKeysLocked(klog.Txn))
					} else {
						kv.commitWithKVLogLocked(klog)
					}
					kv.Unlock()
				case txn.TxnStatus_Committing:
					kv.Lock()
					newTxn := kv.changeUncommittedTxnStatusLocked(klog.Txn.ID, txn.TxnStatus_Committing)
					newTxn.CommitTS = klog.Txn.CommitTS
					kv.Unlock()
				default:
					panic(fmt.Sprintf("invalid txn status %s", klog.Txn.Status.String()))
				}

				c <- klog.Txn
			}
		}

		if lsn == kv.recoverFrom {
			return
		}
	}
}

func (kv *KVTxnStorage) Start() error {
	return nil
}

func (kv *KVTxnStorage) Close(ctx context.Context) error {
	return nil
}

func (kv *KVTxnStorage) Destroy(ctx context.Context) error {
	return nil
}

func (kv *KVTxnStorage) Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (storage.ReadResult, error) {
	kv.RLock()
	defer kv.RUnlock()

	req := &message{}
	req.mustUnmarshal(payload)

	result := newReadResult(req.Keys, txnMeta, kv.continueRead)
	for idx, key := range req.Keys {
		if t, ok := kv.uncommittedKeyTxnMap[string(key)]; ok && needWait(*t, txnMeta) {
			result.waitTxns = append(result.waitTxns, t.ID)
			result.unreaded = append(result.unreaded, idx)
			continue
		}

		result.values[idx] = kv.readValue(key, txnMeta)
	}
	return result, nil
}

func (kv *KVTxnStorage) continueRead(rs *readResult) bool {
	kv.RLock()
	defer kv.RUnlock()

	if len(rs.unreaded) == 0 {
		return true
	}

	for _, idx := range rs.unreaded {
		key := rs.keys[idx]
		txnMeta := rs.txnMeta
		if t, ok := kv.uncommittedKeyTxnMap[string(key)]; ok && needWait(*t, txnMeta) {
			return false
		}

		rs.values[idx] = kv.readValue(key, txnMeta)
	}
	return true
}

func (kv *KVTxnStorage) readValue(key []byte, txnMeta txn.TxnMeta) []byte {
	if t, ok := kv.uncommittedKeyTxnMap[string(key)]; ok && bytes.Equal(t.ID, txnMeta.ID) {
		if v, ok := kv.uncommitted.Get(key); ok {
			return v
		}
	}

	var value []byte
	kv.committed.AscendRange(key, timestamp.Timestamp{}, txnMeta.SnapshotTS, func(v []byte, _ timestamp.Timestamp) {
		value = v
	})
	return value
}

func (kv *KVTxnStorage) Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error) {
	kv.Lock()
	defer kv.Unlock()

	req := &message{}
	req.mustUnmarshal(payload)

	newTxn := txnMeta
	for idx, key := range req.Keys {
		if t, ok := kv.uncommittedKeyTxnMap[string(key)]; ok {
			if !bytes.Equal(t.ID, txnMeta.ID) {
				return nil, moerr.NewTxnWriteConflictNoCtx("%s %s", t.ID, txnMeta.ID)
			}
		} else {
			kv.uncommittedKeyTxnMap[string(key)] = &newTxn
		}

		if _, ok := kv.uncommittedTxn[string(txnMeta.ID)]; !ok {
			kv.uncommittedTxn[string(txnMeta.ID)] = &newTxn
		}

		kv.uncommitted.Set(key, req.Values[idx])
	}
	return nil, nil
}

func (kv *KVTxnStorage) Prepare(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
	kv.Lock()
	defer kv.Unlock()

	if _, ok := kv.uncommittedTxn[string(txnMeta.ID)]; !ok {
		return timestamp.Timestamp{}, moerr.NewMissingTxnNoCtx()
	}

	txnMeta.PreparedTS, _ = kv.clock.Now()
	writeKeys := kv.getWriteKeysLocked(txnMeta)
	if kv.hasConflict(txnMeta.SnapshotTS,
		timestamp.Timestamp{PhysicalTime: math.MaxInt64, LogicalTime: math.MaxUint32},
		writeKeys) {
		return timestamp.Timestamp{}, moerr.NewTxnWriteConflictNoCtx("")
	}

	log := kv.getLogWithDataLocked(txnMeta)
	log.Txn.Status = txn.TxnStatus_Prepared
	lsn, err := kv.saveLog(log)
	if err != nil {
		return timestamp.Timestamp{}, err
	}

	newTxn := kv.changeUncommittedTxnStatusLocked(txnMeta.ID, txn.TxnStatus_Prepared)
	newTxn.PreparedTS = txnMeta.PreparedTS
	newTxn.DNShards = txnMeta.DNShards
	kv.recoverFrom = lsn
	kv.eventC <- Event{Txn: *newTxn, Type: PrepareType}
	return txnMeta.PreparedTS, nil
}

func (kv *KVTxnStorage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error {
	kv.Lock()
	defer kv.Unlock()

	if _, ok := kv.uncommittedTxn[string(txnMeta.ID)]; !ok {
		return moerr.NewMissingTxnNoCtx()
	}

	log := &KVLog{Txn: txnMeta}
	log.Txn.Status = txn.TxnStatus_Committing
	lsn, err := kv.saveLog(log)
	if err != nil {
		return err
	}

	newTxn := kv.changeUncommittedTxnStatusLocked(txnMeta.ID, txn.TxnStatus_Committing)
	newTxn.CommitTS = txnMeta.CommitTS
	kv.recoverFrom = lsn
	kv.eventC <- Event{Txn: *newTxn, Type: CommittingType}
	return nil
}

func (kv *KVTxnStorage) Commit(ctx context.Context, txnMeta txn.TxnMeta) error {
	kv.Lock()
	defer kv.Unlock()

	if _, ok := kv.uncommittedTxn[string(txnMeta.ID)]; !ok {
		return nil
	}

	writeKeys := kv.getWriteKeysLocked(txnMeta)
	if kv.hasConflict(txnMeta.SnapshotTS, txnMeta.CommitTS.Next(), writeKeys) {
		return moerr.NewTxnWriteConflictNoCtx("")
	}

	var log *KVLog
	if txnMeta.Status == txn.TxnStatus_Active {
		log = kv.getLogWithDataLocked(txnMeta)
	} else if txnMeta.Status == txn.TxnStatus_Prepared ||
		txnMeta.Status == txn.TxnStatus_Committing {
		log = &KVLog{Txn: txnMeta}
	} else {
		panic(fmt.Sprintf("commit with invalid status: %s", txnMeta.Status))
	}
	log.Txn.Status = txn.TxnStatus_Committed
	lsn, err := kv.saveLog(log)
	if err != nil {
		return err
	}

	kv.commitKeysLocked(txnMeta, writeKeys)
	kv.recoverFrom = lsn
	kv.eventC <- Event{Txn: log.Txn, Type: CommitType}
	return nil
}

func (kv *KVTxnStorage) Rollback(ctx context.Context, txnMeta txn.TxnMeta) error {
	kv.Lock()
	defer kv.Unlock()

	if _, ok := kv.uncommittedTxn[string(txnMeta.ID)]; !ok {
		return nil
	}

	var writeKeys [][]byte
	for k, v := range kv.uncommittedKeyTxnMap {
		if bytes.Equal(v.ID, txnMeta.ID) {
			writeKeys = append(writeKeys, []byte(k))
		}
	}

	for _, key := range writeKeys {
		kv.uncommitted.Delete(key)
		delete(kv.uncommittedKeyTxnMap, string(key))
	}

	delete(kv.uncommittedTxn, string(txnMeta.ID))
	kv.eventC <- Event{Txn: txnMeta, Type: RollbackType}
	return nil
}

func (kv *KVTxnStorage) Debug(ctx context.Context, meta txn.TxnMeta, op uint32, data []byte) ([]byte, error) {
	return data, nil
}

func (kv *KVTxnStorage) getLogWithDataLocked(txnMeta txn.TxnMeta) *KVLog {
	log := &KVLog{Txn: txnMeta}
	for k, v := range kv.uncommittedKeyTxnMap {
		if bytes.Equal(v.ID, txnMeta.ID) {
			log.Keys = append(log.Keys, []byte(k))
		}
	}
	if len(log.Keys) == 0 {
		panic("commit empty write set")
	}

	for _, key := range log.Keys {
		v, ok := kv.uncommitted.Get(key)
		if !ok {
			panic("missing write set")
		}

		log.Values = append(log.Values, v)
	}
	return log
}

func (kv *KVTxnStorage) hasConflict(from, to timestamp.Timestamp, writeKeys [][]byte) bool {
	for _, key := range writeKeys {
		n := 0
		kv.committed.AscendRange(key, from, to, func(_ []byte, _ timestamp.Timestamp) {
			n++
		})
		if n > 0 {
			return true
		}
	}
	return false
}

func (kv *KVTxnStorage) getWriteKeysLocked(txnMeta txn.TxnMeta) [][]byte {
	var writeKeys [][]byte
	for k, v := range kv.uncommittedKeyTxnMap {
		if bytes.Equal(v.ID, txnMeta.ID) {
			writeKeys = append(writeKeys, []byte(k))
		}
	}
	return writeKeys
}

func (kv *KVTxnStorage) saveLog(log *KVLog) (logservice.Lsn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	data := log.MustMarshal()
	record := kv.logClient.GetLogRecord(len(data))
	if len(record.Data) == 0 {
		record.Data = data
	} else {
		copy(record.Data[len(record.Data)-len(data):], data)
	}
	return kv.logClient.Append(ctx, record)
}

func (kv *KVTxnStorage) commitWithKVLogLocked(klog *KVLog) {
	for idx := range klog.Keys {
		key := klog.Keys[idx]
		value := klog.Values[idx]
		kv.committed.Set(key, klog.Txn.CommitTS, value)
	}
}

func (kv *KVTxnStorage) commitKeysLocked(txnMeta txn.TxnMeta, keys [][]byte) {
	for _, key := range keys {
		v, ok := kv.uncommitted.Get(key)
		if !ok {
			panic("missing write set")
		}

		kv.uncommitted.Delete(key)
		delete(kv.uncommittedKeyTxnMap, string(key))
		kv.committed.Set(key, txnMeta.CommitTS, v)
	}
	delete(kv.uncommittedTxn, string(txnMeta.ID))
}

func (kv *KVTxnStorage) changeUncommittedTxnStatusLocked(id []byte, status txn.TxnStatus) *txn.TxnMeta {
	newTxn := kv.uncommittedTxn[string(id)]
	newTxn.Status = status
	return newTxn
}

func needWait(writeTxn, readTxn txn.TxnMeta) bool {
	if bytes.Equal(writeTxn.ID, readTxn.ID) {
		return false
	}

	switch writeTxn.Status {
	case txn.TxnStatus_Prepared:
		return readTxn.SnapshotTS.Greater(writeTxn.PreparedTS)
	case txn.TxnStatus_Committing:
		return readTxn.SnapshotTS.Greater(writeTxn.CommitTS)
	}
	return false
}

var (
	setOpCode uint32 = 1
	getOpCode uint32 = 2
)

type message struct {
	Keys   [][]byte `json:"key,omitempty"`
	Values [][]byte `json:"value,omitempty"`
}

func (r *message) mustUnmarshal(payload []byte) {
	if err := json.Unmarshal(payload, r); err != nil {
		panic(err)
	}
}

func (r *message) mustMarshal() []byte {
	v, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return v
}

type readResult struct {
	txnMeta          txn.TxnMeta
	keys             [][]byte
	waitTxns         [][]byte
	values           [][]byte
	unreaded         []int
	continueReadFunc func(rs *readResult) bool
}

func newReadResult(keys [][]byte, txnMeta txn.TxnMeta, continueReadFunc func(rs *readResult) bool) *readResult {
	return &readResult{
		keys:             keys,
		values:           make([][]byte, len(keys)),
		continueReadFunc: continueReadFunc,
		txnMeta:          txnMeta,
	}
}

func (rs *readResult) WaitTxns() [][]byte {
	return rs.waitTxns
}

func (rs *readResult) Read() ([]byte, error) {
	if !rs.continueReadFunc(rs) {
		return nil, moerr.NewMissingTxnNoCtx()
	}

	resp := &message{Values: rs.values}
	return resp.mustMarshal(), nil
}

func (rs *readResult) Release() {

}
