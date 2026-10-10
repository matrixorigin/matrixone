// Copyright 2022 Matrix Origin
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

package lockservice

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

type delayedPublication struct {
	io.Reader
	entered, release chan struct{}
	once             sync.Once
}

func (r *delayedPublication) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered); <-r.release })
	return r.Reader.Read(p)
}

func TestPublicationSurvivesAllocatorRestart(t *testing.T) {
	runLockServiceTestsWithLevel(t, zapcore.ErrorLevel, []string{"s1", "s2"}, time.Hour, func(alloc *lockTableAllocator, services []*service) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		a, b := services[0], services[1]
		const table uint64 = 29759
		_, err := a.Lock(ctx, table, newTestRows(1), newTestTxnID(1), newTestRowExclusiveOptions())
		require.NoError(t, err)
		oldBind := a.tableGroups.get(0, table).getBind()
		root := t.TempDir()
		fs, err := fileservice.NewLocalETLFS("etl", root)
		require.NoError(t, err)
		defer fs.Close(ctx)
		reader := &delayedPublication{Reader: bytes.NewBufferString("A"), entered: make(chan struct{}), release: make(chan struct{})}
		var release sync.Once
		defer release.Do(func() { close(reader.release) })
		done := make(chan error, 1)
		go func() {
			done <- fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{FilePath: "manifest.json", Entries: []fileservice.IOEntry{{ReaderForWrite: reader, Size: 1}}})
		}()
		select {
		case <-reader.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		alloc.mu.Lock()
		alloc.mu.services = make(map[string]*serviceBinds)
		alloc.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
		alloc.version++
		alloc.mu.Unlock()
		_, err = b.Lock(ctx, table, newTestRows(1), newTestTxnID(2), newTestRowExclusiveOptions())
		require.NoError(t, err)
		for _, file := range []string{"manifest.json", "READY"} {
			require.NoError(t, fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{FilePath: file, Entries: []fileservice.IOEntry{{Data: []byte("B"), Size: 1}}}))
		}
		invalid, err := alloc.Valid(b.serviceID, newTestTxnID(2), []pb.LockTable{b.tableGroups.get(0, table).getBind()})
		require.NoError(t, err)
		require.Empty(t, invalid)
		require.NoError(t, b.Unlock(ctx, newTestTxnID(2), timestamp.Timestamp{}))
		release.Do(func() { close(reader.release) })
		require.True(t, moerr.IsMoErrCode(<-done, moerr.ErrFileAlreadyExists))
		data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
		require.NoError(t, err)
		require.Equal(t, "B", string(data))
		invalid, err = alloc.Valid(a.serviceID, newTestTxnID(1), []pb.LockTable{oldBind})
		require.NoError(t, err)
		require.Equal(t, []uint64{table}, invalid)
		require.NoError(t, a.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))
	}, nil)
}
