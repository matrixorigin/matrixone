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

const testAccountID = uint32(7)

func TestCASPutStoresAndReturnsSha256(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	data := []byte("hello datalink")

	hash, err := CASPut(ctx, fs, testAccountID, data)
	require.NoError(t, err)

	sum := sha256.Sum256(data)
	require.Equal(t, hex.EncodeToString(sum[:]), hash)

	// the stored CAS object must be byte-identical to the input
	vec := &fileservice.IOVector{
		FilePath: CASKey(testAccountID, hash),
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}},
	}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, data, vec.Entries[0].Data)
}

func TestCASPutIsIdempotent(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	data := []byte("same content")

	h1, err := CASPut(ctx, fs, testAccountID, data)
	require.NoError(t, err)
	// write-once: a second Put of identical bytes must not fail with ErrFileAlreadyExists
	h2, err := CASPut(ctx, fs, testAccountID, data)
	require.NoError(t, err)
	require.Equal(t, h1, h2)
}

func TestCASPutDifferentBytesDifferentHash(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)

	h1, err := CASPut(ctx, fs, testAccountID, []byte("alpha"))
	require.NoError(t, err)
	h2, err := CASPut(ctx, fs, testAccountID, []byte("beta"))
	require.NoError(t, err)
	require.NotEqual(t, h1, h2)
}

func TestCASExists(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	data := []byte("exists check")
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	ok, err := CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.False(t, ok)

	_, err = CASPut(ctx, fs, testAccountID, data)
	require.NoError(t, err)

	ok, err = CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCASKey(t *testing.T) {
	require.Equal(t, "datalink_cas/7/ab/abcd1234ef", CASKey(7, "abcd1234ef"))
}

func TestCASDeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	data := []byte("to delete")
	hash, err := CASPut(ctx, fs, testAccountID, data)
	require.NoError(t, err)

	require.NoError(t, CASDelete(ctx, fs, testAccountID, hash))
	ok, err := CASExists(ctx, fs, testAccountID, hash)
	require.NoError(t, err)
	require.False(t, ok)
	// deleting a non-existent object is a success (idempotent)
	require.NoError(t, CASDelete(ctx, fs, testAccountID, hash))
}

func TestCASListAccount(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	h1, _ := CASPut(ctx, fs, testAccountID, []byte("a"))
	h2, _ := CASPut(ctx, fs, testAccountID, []byte("bb"))
	// another account's object must not appear
	_, _ = CASPut(ctx, fs, uint32(999), []byte("other"))

	got := map[string]bool{}
	for e, err := range CASListAccount(ctx, fs, testAccountID) {
		require.NoError(t, err)
		got[e.Hash] = true
	}
	require.True(t, got[h1])
	require.True(t, got[h2])
	require.Len(t, got, 2)
}

func TestCASDeleteAccountPrefix(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	h1, _ := CASPut(ctx, fs, testAccountID, []byte("a"))
	h2, _ := CASPut(ctx, fs, testAccountID, []byte("bb"))
	keep, _ := CASPut(ctx, fs, uint32(999), []byte("other"))

	require.NoError(t, CASDeleteAccountPrefix(ctx, fs, testAccountID))
	for _, h := range []string{h1, h2} {
		ok, err := CASExists(ctx, fs, testAccountID, h)
		require.NoError(t, err)
		require.False(t, ok)
	}
	// other account is unaffected
	ok, err := CASExists(ctx, fs, uint32(999), keep)
	require.NoError(t, err)
	require.True(t, ok)
}

// CAS objects are namespaced per account: bytes pinned by one account are not
// visible to another account by hash alone. This is the core fix for the bearer
// capability concern — a contenthash is no longer a cross-account read token.
func TestCASAccountIsolation(t *testing.T) {
	ctx := context.Background()
	fs := newTestCASFS(t)
	data := []byte("tenant private bytes")

	const accountA = uint32(100)
	const accountB = uint32(200)

	hash, err := CASPut(ctx, fs, accountA, data)
	require.NoError(t, err)

	// same hash, different account namespaces -> different keys
	require.NotEqual(t, CASKey(accountA, hash), CASKey(accountB, hash))

	// account A can see its own object
	ok, err := CASExists(ctx, fs, accountA, hash)
	require.NoError(t, err)
	require.True(t, ok)

	// account B cannot read account A's object by hash alone
	ok, err = CASExists(ctx, fs, accountB, hash)
	require.NoError(t, err)
	require.False(t, ok)
}
