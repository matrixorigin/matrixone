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

package db

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/stretchr/testify/require"
)

type delayedWALAppendClient struct {
	logservice.Client
	started chan<- struct{}
	release <-chan struct{}
}

func (c *delayedWALAppendClient) Append(ctx context.Context, record logservice.LogRecord) (uint64, error) {
	close(c.started)
	<-c.release
	return c.Client.Append(ctx, record)
}

func TestWriterLockRetainedUntilWALShutdown(t *testing.T) {
	dir := t.TempDir()
	alias := filepath.Join(t.TempDir(), "writer")
	require.NoError(t, os.Symlink(dir, alias))
	lock, err := createDBLock(dir)
	require.NoError(t, err)

	backend, factory := logservicedriver.NewMockServiceAndClientFactory()
	started, release := make(chan struct{}), make(chan struct{})
	allowAppend := sync.OnceFunc(func() { close(release) })
	defer allowAppend()
	cfg := logservicedriver.NewConfig("", logservicedriver.WithConfigOptMaxClient(1),
		logservicedriver.WithConfigOptClientFactory(func() (logservice.Client, error) {
			client, err := factory()
			return &delayedWALAppendClient{Client: client, started: started, release: release}, err
		}))
	cfg.CloseTimeout = 50 * time.Millisecond
	old := wal.NewStore(logservicedriver.NewLogServiceDriver(&cfg))
	mode := func() driver.ReplayMode { return driver.ReplayMode_ReplayForWrite }
	require.NoError(t, old.Replay(t.Context(), nil, mode, nil))

	first := entry.GetBase()
	first.SetType(entry.IOET_WALEntry_Test)
	require.NoError(t, first.SetPayload([]byte("first")))
	lsn, err := old.AppendEntry(wal.GroupUserTxn, first)
	require.NoError(t, err)
	require.Equal(t, uint64(1), lsn)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("append did not reach LogService")
	}

	require.ErrorIs(t, old.Close(), context.DeadlineExceeded)
	require.NoError(t, closeDBLock(lock, old))
	rejected, err := createDBLock(alias)
	require.Error(t, err, "replacement acquired the lock while append was in flight")
	require.Nil(t, rejected)

	allowAppend()
	var newLock io.Closer
	require.Eventually(t, func() bool {
		newLock, err = createDBLock(dir)
		return err == nil
	}, time.Second, time.Millisecond)
	defer newLock.Close()
	require.NoError(t, first.WaitDone())
	first.Free()

	nextCfg := logservicedriver.NewConfig("", logservicedriver.WithConfigOptMaxClient(1), logservicedriver.WithConfigOptClientFactory(factory))
	next := wal.NewStore(logservicedriver.NewLogServiceDriver(&nextCfg))
	var replayed []string
	require.NoError(t, next.Replay(t.Context(), func(_ uint32, _ uint64, payload []byte, _ uint16, _ any) driver.ReplayEntryState {
		replayed = append(replayed, string(payload))
		return driver.RE_Nomal
	}, mode, nil))
	require.Equal(t, []string{"first"}, replayed)
	second := entry.GetBase()
	second.SetType(entry.IOET_WALEntry_Test)
	require.NoError(t, second.SetPayload([]byte("second")))
	lsn, err = next.AppendEntry(wal.GroupUserTxn, second)
	require.NoError(t, err)
	require.Equal(t, uint64(2), lsn)
	require.NoError(t, second.WaitDone())
	require.NoError(t, next.Close())
	second.Free()

	records, _, err := backend.Read(t.Context(), 1, logservicedriver.DefaultRecordSize)
	require.NoError(t, err)
	require.Len(t, records, 2)
	for i, record := range records {
		require.Equal(t, uint64(i+1), logservicedriver.LogEntry(record.Payload()).GetStartDSN())
	}
}
