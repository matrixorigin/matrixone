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

package fileservice

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLocalFS2 runs the standard FileService conformance suite against the
// checksum-free (DISK-V2) LocalFS, to confirm functional parity with the
// checksummed backend.
func TestLocalFS2(t *testing.T) {
	t.Run("file service", func(t *testing.T) {
		testFileService(t, 0, func(name string) FileService {
			ctx := context.Background()
			dir := t.TempDir()
			fs, err := NewLocalFS2(ctx, name, dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("mutable file service", func(t *testing.T) {
		testMutableFileService(t, func() MutableFileService {
			ctx := context.Background()
			dir := t.TempDir()
			fs, err := NewLocalFS2(ctx, "local", dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("replaceable file service", func(t *testing.T) {
		testReplaceableFileService(t, func() ReplaceableFileService {
			ctx := context.Background()
			dir := t.TempDir()
			fs, err := NewLocalFS2(ctx, "local", dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})
}

// writeOne writes content to filePath through fs.
func writeOne(t *testing.T, fs FileService, filePath string, content []byte) {
	t.Helper()
	ctx := context.Background()
	err := fs.Write(ctx, IOVector{
		FilePath: filePath,
		Entries: []IOEntry{
			{Offset: 0, Size: int64(len(content)), Data: content},
		},
	})
	require.NoError(t, err)
}

// readOne reads the whole file content through fs.
func readOne(fs FileService, filePath string, size int64) (IOVector, error) {
	ctx := context.Background()
	vec := IOVector{
		FilePath: filePath,
		Entries:  []IOEntry{{Offset: 0, Size: size}},
	}
	err := fs.Read(ctx, &vec)
	return vec, err
}

// TestLocalFS2RawFormat proves that DISK-V2 stores raw bytes: the on-disk file
// equals the written content exactly (no CRC32 framing, no size inflation),
// which is byte-identical to how S3FS (disk-backed) stores objects.
func TestLocalFS2RawFormat(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := NewLocalFS2(ctx, "local", dir, DisabledCacheConfig, nil)
	require.NoError(t, err)

	content := make([]byte, 9000) // > 4 blocks worth at 2044B
	_, err = rand.Read(content)
	require.NoError(t, err)
	writeOne(t, fs, "foobar", content)

	onDisk, err := os.ReadFile(filepath.Join(dir, toOSPath("foobar")))
	require.NoError(t, err)
	require.Equal(t, len(content), len(onDisk), "no CRC inflation")
	require.True(t, bytes.Equal(content, onDisk), "on-disk bytes == written bytes")

	// StatFile / read report the raw size
	entry, err := fs.StatFile(ctx, "foobar")
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), entry.Size)

	vec, err := readOne(fs, "foobar", int64(len(content)))
	require.NoError(t, err)
	require.True(t, bytes.Equal(content, vec.Entries[0].Data))
}

// TestLocalFSChecksumInflation is the contrast case: the legacy DISK backend
// interleaves a 4-byte CRC32 per 2044-byte block, so the on-disk file is larger
// than the content.
func TestLocalFSChecksumInflation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := NewLocalFS(ctx, "local", dir, DisabledCacheConfig, nil)
	require.NoError(t, err)

	content := make([]byte, 9000)
	_, err = rand.Read(content)
	require.NoError(t, err)
	writeOne(t, fs, "foobar", content)

	onDisk, err := os.ReadFile(filepath.Join(dir, toOSPath("foobar")))
	require.NoError(t, err)
	require.Greater(t, len(onDisk), len(content), "checksummed format inflates size")
}

// TestLocalFSFormatsAreIncompatible documents that the two formats cannot be
// mixed on the same data directory, since neither has a magic/header:
//   - DISK reading a DISK-V2 (raw) file fails loudly with a checksum error.
//   - DISK-V2 reading a DISK (checksummed) file returns the raw bytes including
//     the interleaved CRCs (silent corruption — there is no check to fail).
func TestLocalFSFormatsAreIncompatible(t *testing.T) {
	ctx := context.Background()

	content := make([]byte, 6000)
	_, err := rand.Read(content)
	require.NoError(t, err)

	// raw-written, checksum-read -> loud failure
	t.Run("raw write, checksum read", func(t *testing.T) {
		dir := t.TempDir()
		rawFS, err := NewLocalFS2(ctx, "local", dir, DisabledCacheConfig, nil)
		require.NoError(t, err)
		writeOne(t, rawFS, "f", content)

		crcFS, err := NewLocalFS(ctx, "local", dir, DisabledCacheConfig, nil)
		require.NoError(t, err)
		_, err = readOne(crcFS, "f", int64(len(content)))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "expected checksum mismatch, got %v", err)
	})

	// checksum-written, raw-read -> wrong bytes, no error (silent corruption)
	t.Run("checksum write, raw read", func(t *testing.T) {
		dir := t.TempDir()
		crcFS, err := NewLocalFS(ctx, "local", dir, DisabledCacheConfig, nil)
		require.NoError(t, err)
		writeOne(t, crcFS, "f", content)

		// physical file is larger than content; read that many raw bytes
		onDisk, err := os.ReadFile(filepath.Join(dir, toOSPath("f")))
		require.NoError(t, err)
		rawFS, err := NewLocalFS2(ctx, "local", dir, DisabledCacheConfig, nil)
		require.NoError(t, err)
		vec, err := readOne(rawFS, "f", int64(len(onDisk)))
		require.NoError(t, err) // no error: raw mode has nothing to validate
		require.False(t, bytes.Equal(content, vec.Entries[0].Data[:len(content)]),
			"raw read of checksummed data must not match the original content")
	})
}
