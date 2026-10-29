// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package python

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/stretchr/testify/require"
)

func newTestArtifactStore(t *testing.T, maxSize int64) *FileArtifactStore {
	t.Helper()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	store, err := NewFileArtifactStore(fs, maxSize)
	require.NoError(t, err)
	return store
}

func TestFileArtifactStorePublishesAndResolvesByAccount(t *testing.T) {
	store := newTestArtifactStore(t, DefaultMaxArtifactBytes)
	ctx := context.Background()
	source := "def add(ctx, value):\n    return value + 1\n"
	digest := udf.PythonInlineArtifactDigest("add", source)

	published, err := store.Publish(ctx, 7, "add", source)
	require.NoError(t, err)
	require.Equal(t, digest, published)
	resolved, err := store.Resolve(ctx, 7, "add", digest)
	require.NoError(t, err)
	require.Equal(t, source, resolved)

	_, err = store.Resolve(ctx, 8, "add", digest)
	require.ErrorContains(t, err, "unavailable")
	second, err := store.Publish(ctx, 7, "add", source)
	require.NoError(t, err)
	require.Equal(t, digest, second, "republication of the exact immutable object is idempotent")
}

func TestFileArtifactStoreRejectsCorruptionAndDigestCollision(t *testing.T) {
	store := newTestArtifactStore(t, DefaultMaxArtifactBytes)
	ctx := context.Background()
	source := "def add(ctx, value): return value + 1"
	digest := udf.PythonInlineArtifactDigest("add", source)

	path := artifactPath(7, digest)
	err := store.fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len("corrupt")), Data: []byte("corrupt")}},
	})
	require.NoError(t, err)
	_, err = store.Resolve(ctx, 7, "add", digest)
	require.ErrorContains(t, err, "content digest mismatch")
	_, err = store.Publish(ctx, 7, "add", source)
	require.ErrorContains(t, err, "invalid existing object")
}

func TestFileArtifactStoreRejectsOversizedSourceBeforeWrite(t *testing.T) {
	store := newTestArtifactStore(t, 4)
	_, err := store.Publish(context.Background(), 7, "add", "12345")
	require.ErrorContains(t, err, "exceeds 4 bytes")
	_, statErr := store.fs.StatFile(context.Background(), artifactPath(7, udf.PythonInlineArtifactDigest("add", "12345")))
	require.Error(t, statErr)
}

func TestNewFileArtifactStoreRequiresSharedFileService(t *testing.T) {
	local, err := fileservice.NewMemoryFS("LOCAL", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	_, err = NewFileArtifactStore(local, 0)
	require.ErrorContains(t, err, "shared FileService")

	shared, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	_, err = NewFileArtifactStore(shared, DefaultMaxArtifactBytes+1)
	require.ErrorContains(t, err, "exceeds the current contract limit")
}

func TestFileArtifactStoreRejectsInvalidDirectConstruction(t *testing.T) {
	var nilStore *FileArtifactStore
	_, err := nilStore.Publish(context.Background(), 7, "add", "source")
	require.ErrorContains(t, err, "no FileService")
	_, err = nilStore.Resolve(context.Background(), 7, "add", strings.Repeat("a", 64))
	require.ErrorContains(t, err, "no FileService")

	store := &FileArtifactStore{}
	_, err = store.Publish(context.Background(), 7, "add", "source")
	require.ErrorContains(t, err, "no FileService")
}

func TestFileArtifactStoreRejectsInvalidUTF8(t *testing.T) {
	store := newTestArtifactStore(t, DefaultMaxArtifactBytes)
	_, err := store.Publish(context.Background(), 7, "add", string([]byte{'\xff'}))
	require.ErrorContains(t, err, "not valid UTF-8")
}

// The wrapper deliberately has no recognized exclusive-write capability.
type readOnlyArtifactFS struct{ fileservice.FileService }

func TestFileArtifactStorePreservesLegacyReadWithoutExclusiveWriter(t *testing.T) {
	legacy := newTestArtifactStore(t, DefaultMaxArtifactBytes)
	const source = "def f(ctx): return 7"
	digest := udf.PythonInlineArtifactDigest("f", source)
	// An existing artifact from before admission is an immutable baseline.
	require.NoError(t, legacy.fs.Write(t.Context(), fileservice.IOVector{
		FilePath: artifactPath(7, digest),
		Entries:  []fileservice.IOEntry{{Size: int64(len(source)), Data: []byte(source)}},
	}))
	store, err := NewFileArtifactStore(readOnlyArtifactFS{legacy.fs}, DefaultMaxArtifactBytes)
	require.NoError(t, err)
	resolved, err := store.Resolve(t.Context(), 7, "f", digest)
	require.NoError(t, err)
	require.Equal(t, source, resolved)
	_, err = store.Publish(t.Context(), 7, "f", source)
	require.NoError(t, err, "existing objects do not require new reservations")
	_, err = store.Publish(t.Context(), 7, "f", "def f(ctx): return 8")
	require.ErrorContains(t, err, "requires exclusive publication")
	for _, err := range legacy.fs.List(t.Context(), "udf-artifact-admission") {
		require.NoError(t, err)
		t.Fatal("unsupported backend must not reserve capacity")
	}
}

func TestFileArtifactStoreAdmissionRecoveryAndSharedDigest(t *testing.T) {
	store := newTestArtifactStore(t, DefaultMaxArtifactBytes)
	store.maxPublications = 1
	ctx := t.Context()
	source := "def f(ctx): return 1"
	digest := udf.PythonInlineArtifactDigest("f", source)
	// Simulate a crash after admission but before writing any source. The
	// durable reservation must be reusable by another CN instance.
	identity := fmt.Sprintf("7/%s", digest)
	require.NoError(t, fileservice.WriteExclusive(ctx, store.fs, fileservice.IOVector{
		FilePath: artifactAdmissionPath(0),
		Entries:  []fileservice.IOEntry{{Size: int64(len(identity)), Data: []byte(identity)}},
	}))
	restarted, err := NewFileArtifactStore(store.fs, DefaultMaxArtifactBytes)
	require.NoError(t, err)
	restarted.maxPublications = 1
	_, err = restarted.Publish(ctx, 7, "f", source)
	require.NoError(t, err)
	// A failed catalog transaction does not release this identity: a second
	// committed revision or a snapshot can already depend on the same digest.
	_, err = store.Publish(ctx, 7, "f", source)
	require.NoError(t, err)
	_, err = restarted.Publish(ctx, 8, "f", source)
	require.ErrorContains(t, err, "publication budget")
	_, err = restarted.Publish(ctx, 7, "f", "def f(ctx): return 2")
	require.ErrorContains(t, err, "publication budget")
	resolved, err := restarted.Resolve(ctx, 7, "f", digest)
	require.NoError(t, err)
	require.Equal(t, source, resolved)
	for _, dir := range []string{"udf-artifacts/7", "udf-artifact-admission/v1"} {
		count := 0
		for entry, err := range store.fs.List(ctx, dir) {
			require.NoError(t, err)
			require.False(t, entry.IsDir)
			count++
		}
		require.Equal(t, 1, count)
	}
}

func TestFileArtifactStoreConcurrentAdmission(t *testing.T) {
	for _, same := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared_digest_%t", same), func(t *testing.T) {
			first := newTestArtifactStore(t, DefaultMaxArtifactBytes)
			second, err := NewFileArtifactStore(first.fs, DefaultMaxArtifactBytes)
			require.NoError(t, err)
			first.maxPublications, second.maxPublications = 1, 1
			start := make(chan struct{})
			results := make(chan error, 2)
			var wg sync.WaitGroup
			for i, store := range []*FileArtifactStore{first, second} {
				wg.Add(1)
				go func(i int, store *FileArtifactStore) {
					defer wg.Done()
					<-start
					if same {
						i = 0
					}
					_, err := store.Publish(t.Context(), 7, "f", fmt.Sprintf("def f(ctx): return %d", i))
					results <- err
				}(i, store)
			}
			close(start)
			wg.Wait()
			close(results)
			success := 0
			for err := range results {
				if err == nil {
					success++
				} else {
					require.ErrorContains(t, err, "publication budget")
				}
			}
			if same {
				require.Equal(t, 2, success)
			} else {
				require.Equal(t, 1, success)
			}
			count := 0
			for entry, err := range first.fs.List(t.Context(), "udf-artifacts/7") {
				require.NoError(t, err)
				require.False(t, entry.IsDir)
				count++
			}
			require.Equal(t, 1, count, "losing admission must not write another object")
		})
	}
}

func TestFileArtifactStoreAdmissionCapacityTwo(t *testing.T) {
	store := newTestArtifactStore(t, DefaultMaxArtifactBytes)
	store.maxPublications = 2
	for _, source := range []string{"def f(ctx): return 1", "def f(ctx): return 2"} {
		_, err := store.Publish(t.Context(), 7, "f", source)
		require.NoError(t, err)
	}
	_, err := store.Publish(t.Context(), 7, "f", "def f(ctx): return 3")
	require.ErrorContains(t, err, "publication budget")
	for _, dir := range []string{"udf-artifacts/7", "udf-artifact-admission/v1"} {
		count := 0
		for entry, err := range store.fs.List(t.Context(), dir) {
			require.NoError(t, err)
			require.False(t, entry.IsDir)
			count++
		}
		require.Equal(t, 2, count)
	}
}
