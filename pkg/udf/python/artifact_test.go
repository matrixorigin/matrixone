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
	"strings"
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
