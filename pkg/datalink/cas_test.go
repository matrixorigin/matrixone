// Copyright 2024 Matrix Origin
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

package datalink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func newTestCASFS(t *testing.T) fileservice.FileService {
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	return fs
}

func TestCASPutStoresAndReturnsSha256(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	data := []byte("hello datalink")

	hash, err := CASPut(ctx, fs, data)
	require.NoError(t, err)

	sum := sha256.Sum256(data)
	require.Equal(t, hex.EncodeToString(sum[:]), hash)

	// the stored CAS object must be byte-identical to the input
	vec := &fileservice.IOVector{
		FilePath: CASKey(hash),
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}},
	}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, data, vec.Entries[0].Data)
}

func TestCASPutIsIdempotent(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	data := []byte("same content")

	h1, err := CASPut(ctx, fs, data)
	require.NoError(t, err)
	// write-once: a second Put of identical bytes must not fail with ErrFileAlreadyExists
	h2, err := CASPut(ctx, fs, data)
	require.NoError(t, err)
	require.Equal(t, h1, h2)
}

func TestCASPutDifferentBytesDifferentHash(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)

	h1, err := CASPut(ctx, fs, []byte("alpha"))
	require.NoError(t, err)
	h2, err := CASPut(ctx, fs, []byte("beta"))
	require.NoError(t, err)
	require.NotEqual(t, h1, h2)
}

func TestCASExists(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	data := []byte("exists check")
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	ok, err := CASExists(ctx, fs, hash)
	require.NoError(t, err)
	require.False(t, ok)

	_, err = CASPut(ctx, fs, data)
	require.NoError(t, err)

	ok, err = CASExists(ctx, fs, hash)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCASKey(t *testing.T) {
	require.Equal(t, "datalink_cas/ab/abcd1234ef", CASKey("abcd1234ef"))
}
