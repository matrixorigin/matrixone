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
	"encoding/json"
	"io"
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

type publicationManifest struct {
	Version        int                   `json:"version"`
	SourceDatabase string                `json:"source_database"`
	SourceTable    string                `json:"source_table"`
	SchemaHash     string                `json:"schema_hash"`
	MetadataOnly   bool                  `json:"metadata_only"`
	Relations      []publicationRelation `json:"relations"`
}

type publicationRelation struct {
	Role        string              `json:"role"`
	SourceTable string              `json:"source_table"`
	SchemaHash  string              `json:"schema_hash"`
	Objects     []publicationObject `json:"objects"`
}

type publicationObject struct {
	Name   string `json:"name"`
	Size   int64  `json:"size,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
}

func (r *delayedPublication) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered); <-r.release })
	return r.Reader.Read(p)
}

func TestPublicationSurvivesAllocatorRestart(t *testing.T) {
	runLockServiceRestartTests(t, zapcore.ErrorLevel, []string{"s1", "s2"}, time.Hour, func(alloc *lockTableAllocator, services []*service, restart func(...AllocatorOption) (*lockTableAllocator, error)) {
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
		payloads := map[string][]byte{
			"objects/A": []byte("rows from publisher A"),
			"objects/B": []byte("rows from publisher B"),
		}
		for path, payload := range payloads {
			require.NoError(t, fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{
				FilePath: path,
				Entries:  []fileservice.IOEntry{{Data: payload, Size: int64(len(payload))}},
			}))
		}
		manifestFor := func(sourceTable, objectPath string, payload []byte) []byte {
			data, marshalErr := json.Marshal(publicationManifest{
				Version: 1, SourceDatabase: "db", SourceTable: sourceTable,
				SchemaHash: "schema", Relations: []publicationRelation{{
					Role: "base", SourceTable: sourceTable, SchemaHash: "schema",
					Objects: []publicationObject{{Name: objectPath, Size: int64(len(payload))}},
				}},
			})
			require.NoError(t, marshalErr)
			return data
		}
		manifestA := manifestFor("source_a", "objects/A", payloads["objects/A"])
		manifestB := manifestFor("source_b", "objects/B", payloads["objects/B"])
		reader := &delayedPublication{Reader: bytes.NewReader(manifestA), entered: make(chan struct{}), release: make(chan struct{})}
		var release sync.Once
		done := make(chan struct{})
		var writeErr error
		defer joinDelayedPublication(t, &release, reader.release, cancel, done)
		go func() {
			defer close(done)
			writeErr = fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{FilePath: "manifest.json", Entries: []fileservice.IOEntry{{ReaderForWrite: reader, Size: int64(len(manifestA))}}})
		}()
		select {
		case <-reader.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		// Incarnation fields are immutable while allocator RPCs are running.
		// The restart helper closes the owner and drains stale backends before
		// creating its replacement, rather than mutating a live epoch.
		alloc, err = restart()
		require.NoError(t, err)
		require.NotEqual(t, oldBind.AllocatorID, alloc.allocatorID)
		_, err = b.Lock(ctx, table, newTestRows(1), newTestTxnID(2), newTestRowExclusiveOptions())
		require.NoError(t, err)
		require.NoError(t, fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{
			FilePath: "manifest.json",
			Entries:  []fileservice.IOEntry{{Data: manifestB, Size: int64(len(manifestB))}},
		}))
		require.NoError(t, fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{
			FilePath: "READY",
			Entries:  []fileservice.IOEntry{{Data: []byte("1"), Size: 1}},
		}))
		invalid, err := alloc.Valid(b.serviceID, newTestTxnID(2), []pb.LockTable{b.tableGroups.get(0, table).getBind()})
		require.NoError(t, err)
		require.Empty(t, invalid)
		require.NoError(t, b.Unlock(ctx, newTestTxnID(2), timestamp.Timestamp{}))
		release.Do(func() { close(reader.release) })
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		require.True(t, moerr.IsMoErrCode(writeErr, moerr.ErrFileAlreadyExists))
		manifestVector := &fileservice.IOVector{FilePath: "manifest.json", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(manifestB))}}}
		require.NoError(t, fs.Read(ctx, manifestVector))
		defer manifestVector.Release()
		var published publicationManifest
		require.NoError(t, json.Unmarshal(manifestVector.Entries[0].Data, &published))
		require.Equal(t, "source_b", published.SourceTable)
		require.Len(t, published.Relations, 1)
		require.Len(t, published.Relations[0].Objects, 1)
		objectPath := published.Relations[0].Objects[0].Name
		require.Equal(t, "objects/B", objectPath)
		objectVector := &fileservice.IOVector{FilePath: objectPath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(payloads[objectPath]))}}}
		require.NoError(t, fs.Read(ctx, objectVector))
		defer objectVector.Release()
		require.Equal(t, payloads[objectPath], objectVector.Entries[0].Data)
		_, err = fs.StatFile(ctx, "READY")
		require.NoError(t, err)
		invalid, err = alloc.Valid(a.serviceID, newTestTxnID(1), []pb.LockTable{oldBind})
		require.NoError(t, err)
		require.Equal(t, []uint64{table}, invalid)
		require.NoError(t, a.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))
	}, nil)
}

// Always unblock the reader before joining: context cancellation cannot interrupt
// a ReaderForWrite that is already inside Read.
func joinDelayedPublication(t *testing.T, release *sync.Once, barrier chan struct{}, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	release.Do(func() { close(barrier) })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("delayed publication did not exit during cleanup")
	}
}

func TestDelayedPublicationCleanupAfterEarlyExit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fs, err := fileservice.NewLocalETLFS("etl", t.TempDir())
	require.NoError(t, err)
	defer fs.Close(ctx)
	reader := &delayedPublication{Reader: bytes.NewReader([]byte("data")), entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	done := make(chan struct{})
	func() {
		defer joinDelayedPublication(t, &release, reader.release, cancel, done)
		go func() {
			defer close(done)
			_ = fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{
				FilePath: "manifest.json", Entries: []fileservice.IOEntry{{ReaderForWrite: reader, Size: 4}},
			})
		}()
		select {
		case <-reader.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		// Return while the actual file writer is blocked, as an early assertion
		// exit would; deferred cleanup must join it before closing fs.
	}()
	select {
	case <-done:
	default:
		t.Fatal("publication writer survived cleanup")
	}
}
