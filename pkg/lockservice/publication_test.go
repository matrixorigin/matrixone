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
		defer release.Do(func() { close(reader.release) })
		done := make(chan error, 1)
		go func() {
			done <- fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{FilePath: "manifest.json", Entries: []fileservice.IOEntry{{ReaderForWrite: reader, Size: int64(len(manifestA))}}})
		}()
		select {
		case <-reader.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		// Lose the allocator's volatile bindings while its RPC workers are live.
		// The process epoch is immutable and need not change to make the old
		// binding stale once the other service takes ownership.
		alloc.mu.Lock()
		alloc.mu.services = make(map[string]*serviceBinds)
		alloc.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
		alloc.mu.Unlock()
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
		require.True(t, moerr.IsMoErrCode(<-done, moerr.ErrFileAlreadyExists))
		manifestVector := &fileservice.IOVector{FilePath: "manifest.json", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(manifestB))}}}
		require.NoError(t, fs.Read(ctx, manifestVector))
		var published publicationManifest
		require.NoError(t, json.Unmarshal(manifestVector.Entries[0].Data, &published))
		manifestVector.Release()
		require.Equal(t, "source_b", published.SourceTable)
		require.Len(t, published.Relations, 1)
		require.Len(t, published.Relations[0].Objects, 1)
		objectPath := published.Relations[0].Objects[0].Name
		require.Equal(t, "objects/B", objectPath)
		objectVector := &fileservice.IOVector{FilePath: objectPath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(payloads[objectPath]))}}}
		require.NoError(t, fs.Read(ctx, objectVector))
		require.Equal(t, payloads[objectPath], objectVector.Entries[0].Data)
		objectVector.Release()
		_, err = fs.StatFile(ctx, "READY")
		require.NoError(t, err)
		invalid, err = alloc.Valid(a.serviceID, newTestTxnID(1), []pb.LockTable{oldBind})
		require.NoError(t, err)
		require.Equal(t, []uint64{table}, invalid)
		require.NoError(t, a.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))
	}, nil)
}
