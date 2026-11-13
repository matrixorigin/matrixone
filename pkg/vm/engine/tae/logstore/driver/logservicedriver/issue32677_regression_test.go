// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservicedriver

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	storeDriver "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/stretchr/testify/require"
)

func TestRejectedLogRecordIsSkippedByTNReplay(t *testing.T) {
	const walPayloadHex = "e8030200010000000000000000000000000000000000000000000000000000000000000000000100000001000000000000000000000000000000830000000100000000000000d5070100150000001c0000000000000083010000000000000000000000000000000000000000000072656a65637465642d6275742d7265706c617965643e00000045000000"
	encoded, err := hex.DecodeString(walPayloadHex)
	require.NoError(t, err)
	record := logservice.NewUserLogRecord(100, len(encoded))
	copy(record.Payload(), encoded)
	record.Lsn = 5
	record.Type = pb.LeaseRejected
	backend := NewMockBackend()
	backend.store = []logservice.LogRecord{
		{Lsn: 1, Type: pb.Internal},
		{Lsn: 2, Type: pb.Internal},
		{Lsn: 3, Type: pb.LeaseUpdate},
		{Lsn: 4, Type: pb.LeaseUpdate},
		record,
	}
	backend.nextPSN = 6
	client := newMockBackendClient(backend)
	cfg := NewConfig("", WithConfigOptClientFactory(func() (logservice.Client, error) {
		return client, nil
	}), WithConfigOptMaxClient(1), WithConfigOptMaxTimeout(2*time.Second))
	d := NewLogServiceDriver(&cfg)
	defer func() { require.NoError(t, d.Close()) }()
	var applied []uint64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = d.Replay(ctx, func(e *entry.Entry) storeDriver.ReplayEntryState {
		applied = append(applied, e.DSN)
		return storeDriver.RE_Nomal
	}, func() storeDriver.ReplayMode {
		return storeDriver.ReplayMode_ReplayForWrite
	}, nil)
	require.NoError(t, err)
	require.Empty(t, applied)
}
