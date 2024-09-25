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
	"context"
	"encoding/json"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// KVLog kv log
type KVLog struct {
	Txn    txn.TxnMeta `json:"txn"`
	Keys   [][]byte    `json:"key,omitempty"`
	Values [][]byte    `json:"value,omitempty"`
}

// MustUnmarshal must unmarshal
func (l *KVLog) MustUnmarshal(data []byte) {
	if err := json.Unmarshal(data, l); err != nil {
		panic(l)
	}
}

// MustMarshal must marshal
func (l *KVLog) MustMarshal() []byte {
	data, err := json.Marshal(l)
	if err != nil {
		panic(err)
	}
	return data
}

// NewMemLog new log use memory as backend. Log index is start from 1.
func NewMemLog() logservice.Client {
	return &memLogClient{}
}

type memLogClient struct {
	sync.RWMutex
	logs []logpb.LogRecord
}

func (mc *memLogClient) Close() error { return nil }

func (mc *memLogClient) Config() logservice.ClientConfig { return logservice.ClientConfig{} }

func (mc *memLogClient) GetLogRecord(payloadLength int) logpb.LogRecord {
	return logpb.LogRecord{}
}

func (mc *memLogClient) Append(ctx context.Context, log logpb.LogRecord) (logservice.Lsn, error) {
	mc.Lock()
	defer mc.Unlock()

	log.Lsn = logservice.Lsn(len(mc.logs) + 1)
	mc.logs = append(mc.logs, log)
	return logservice.Lsn(len(mc.logs)), nil
}

func (mc *memLogClient) Read(ctx context.Context, firstIndex logservice.Lsn, maxSize uint64) ([]logpb.LogRecord, logservice.Lsn, error) {
	mc.RLock()
	defer mc.RUnlock()

	if firstIndex > logservice.Lsn(len(mc.logs)) {
		return nil, firstIndex, nil
	}

	values := make([]logpb.LogRecord, logservice.Lsn(len(mc.logs))-firstIndex+1)
	copy(values, mc.logs[firstIndex-1:])
	return values, firstIndex, nil
}

func (mc *memLogClient) Truncate(ctx context.Context, index logservice.Lsn) error {
	mc.Lock()
	defer mc.Unlock()

	if index > logservice.Lsn(len(mc.logs)) {
		panic("invalid truncate index")
	}

	mc.logs = mc.logs[index:]
	return nil
}

func (mc *memLogClient) GetTruncatedLsn(ctx context.Context) (logservice.Lsn, error) {
	return 0, nil
}

func (mc *memLogClient) GetTSOTimestamp(ctx context.Context, count uint64) (uint64, error) {
	return 0, nil
}

func (mc *memLogClient) GetLatestLsn(ctx context.Context) (logservice.Lsn, error) {
	return 0, nil
}

func (mc *memLogClient) SetRequiredLsn(ctx context.Context, lsn logservice.Lsn) error {
	return nil
}

func (mc *memLogClient) GetRequiredLsn(ctx context.Context) (logservice.Lsn, error) {
	return 0, nil
}
