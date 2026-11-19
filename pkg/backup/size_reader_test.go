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

package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestBackupMinioSizePolicy(t *testing.T) {
	ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	for _, flag := range []string{"true", "false", "absent"} {
		t.Run(flag, func(t *testing.T) {
			options := []string{"endpoint", server.URL, "region", "us-east-1", "access_key_id", "test-key", "secret_access_key", "test-secret", "bucket", "backup-" + flag}
			if flag != "absent" {
				options = append(options, "is_minio", flag)
			}
			cfg := &Config{Metas: NewMetas(), copyFiles: backupFileCopier{statSource: true}}
			defer func() {
				if cfg.GeneralDir != nil {
					cfg.GeneralDir.Close(ctx)
				}
				if cfg.TaeDir != nil {
					cfg.TaeDir.Close(ctx)
				}
			}()
			stop := errors.New("checked backup copy policy")
			called := false
			stubs := gostub.Stub(&launchConfigPaths, map[string][]string{})
			defer stubs.Reset()
			stubs.Stub(&backupTae, func(_ context.Context, _ string, c *Config) error {
				called = true
				require.Equal(t, flag == "true", c.copyFiles.statSource)
				return stop
			})
			err := Backup(ctx, "", &tree.BackupStart{IsS3: true, Option: options}, cfg)
			require.ErrorIs(t, err, stop)
			require.True(t, called)
		})
	}
}

func TestBackupSizeReader(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
		size int64
		fail bool
	}{
		{"one", "a", 1, false},
		{"many", "abcdef", 6, false},
		{"short", "abc", 6, true},
		{"empty_short", "", 1, true},
		{"long", "abcdef", 5, true},
		{"one_long", "ab", 1, true},
	} {
		for _, chunk := range []int{1, 2, 32} {
			t.Run(fmt.Sprintf("%s/chunk_%d", tc.name, chunk), func(t *testing.T) {
				r := &backupSizeReader{reader: strings.NewReader(tc.data), remaining: tc.size}
				// Model a consumer that stops after Size bytes and never asks
				// for EOF, just like the FileService/SDK bounded readers.
				limited := io.LimitReader(r, tc.size)
				var got bytes.Buffer
				buf := make([]byte, chunk)
				var err error
				for err == nil {
					var n int
					n, err = limited.Read(buf)
					got.Write(buf[:n])
				}
				if tc.fail {
					require.Error(t, err)
					require.NotEqual(t, io.EOF, err)
					require.Less(t, int64(got.Len()), tc.size)
					if int64(len(tc.data)) > tc.size {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
						require.False(t, fileservice.IsRetryableError(err))
					}
				} else {
					require.ErrorIs(t, err, io.EOF)
					require.Equal(t, tc.data, got.String())
				}
			})
		}
	}
	t.Run("read_full_cannot_swallow_boundary_error", func(t *testing.T) {
		r := &backupSizeReader{reader: strings.NewReader("extra"), remaining: 4}
		n, err := io.ReadFull(r, make([]byte, 4))
		require.Error(t, err)
		require.Equal(t, 3, n)
	})
	t.Run("EOF_check_error", func(t *testing.T) {
		r := &backupSizeReader{reader: io.MultiReader(strings.NewReader("a"), backupErrorReader{}), remaining: 1}
		n, err := r.Read(make([]byte, 1))
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, n)
	})
	for _, terminal := range []error{io.EOF, context.Canceled} {
		t.Run("last_byte_with_"+terminal.Error(), func(t *testing.T) {
			r := &backupSizeReader{reader: backupLastByteReader{terminal}, remaining: 1}
			buf := make([]byte, 1)
			n, err := io.ReadFull(r, buf)
			if terminal == io.EOF {
				require.NoError(t, err)
				require.Equal(t, 1, n)
				require.Equal(t, byte('a'), buf[0])
			} else {
				require.ErrorIs(t, err, terminal)
				require.Zero(t, n)
			}
		})
	}
}

type backupLastByteReader struct{ terminal error }

func (r backupLastByteReader) Read(p []byte) (int, error) {
	p[0] = 'a'
	return 1, r.terminal
}

type backupErrorReader struct{}

func (backupErrorReader) Read([]byte) (int, error) { return 0, context.Canceled }

type backupSizedSource struct {
	fileservice.FileService
	size       int64
	data       string
	statErr    error
	readErr    error
	streamErr  bool
	stats      int
	reads      int
	closes     int
	statName   string
	readName   string
	readOpened bool
}

func (s *backupSizedSource) StatFile(_ context.Context, name string) (*fileservice.DirEntry, error) {
	s.stats++
	s.statName = name
	if s.readOpened {
		return nil, errors.New("Stat must precede opening the source stream")
	}
	return &fileservice.DirEntry{Name: name, Size: s.size}, s.statErr
}

func (s *backupSizedSource) Read(_ context.Context, v *fileservice.IOVector) error {
	s.reads++
	s.readName = v.FilePath
	if s.readErr != nil {
		return s.readErr
	}
	if v.Entries[0].Size != -1 || v.Policy != fileservice.SkipAllCache {
		return errors.New("source must remain an uncached EOF stream")
	}
	s.readOpened = true
	var r io.Reader = strings.NewReader(s.data)
	if s.streamErr {
		r = io.MultiReader(r, backupErrorReader{})
	}
	*v.Entries[0].ReadCloserForRead = &backupSizedReadCloser{Reader: r, source: s}
	return nil
}

type backupSizedReadCloser struct {
	io.Reader
	source *backupSizedSource
}

func (r *backupSizedReadCloser) Close() error {
	r.source.closes++
	r.source.readOpened = false
	return nil
}

type backupSizedTarget struct {
	fileservice.FileService
	size      int64
	name      string
	writes    int
	data      []byte
	writeErr  error
	copyErr   error
	copyCalls int
}

func (s *backupSizedTarget) CopyObject(context.Context, fileservice.FileService, string, string) (bool, error) {
	s.copyCalls++
	return false, s.copyErr
}

func (s *backupSizedTarget) Write(_ context.Context, v fileservice.IOVector) error {
	s.writes++
	s.size = v.Entries[0].Size
	s.name = v.FilePath
	if s.writeErr != nil {
		return s.writeErr
	}
	r := v.Entries[0].ReaderForWrite
	if s.size >= 0 {
		r = io.LimitReader(r, s.size)
	}
	b, err := io.ReadAll(r)
	if err == nil {
		s.data = b
	}
	return err
}

func TestBackupSizedCopy(t *testing.T) {
	t.Run("incremental_skipped_object_does_not_stat_or_copy", func(t *testing.T) {
		name, object := newBackupObject(1)
		object.NeedCopy = false
		src := &backupSizedSource{statErr: errors.New("must not stat")}
		dst := &backupSizedTarget{}
		files, err := (backupFileCopier{statSource: true}).parallelCopyData(
			t.Context(), src, dst, map[string]*objectio.BackupObject{name: object}, 1, nil,
		)
		require.NoError(t, err)
		require.Len(t, files, 1)
		require.False(t, files[0].needCopy)
		require.Zero(t, src.stats)
		require.Zero(t, src.reads)
		require.Zero(t, dst.writes)
	})
	for _, tc := range []struct {
		name string
		data string
		size int64
		fail bool
	}{
		{"empty", "", 0, false},
		{"one", "a", 1, false},
		{"normal", "content", 7, false},
		{"short", "content", 8, true},
		{"long", "content", 6, true},
		{"not_empty", "content", 0, true},
		{"negative", "content", -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &backupSizedSource{size: tc.size, data: tc.data}
			dst := &backupSizedTarget{}
			sum, err := (backupFileCopier{statSource: true}).copyFile(t.Context(), src, dst, "old", "gc", "new")
			require.Equal(t, 1, src.stats)
			require.Equal(t, "gc/old", src.statName)
			require.Equal(t, src.reads, src.closes)
			if tc.fail {
				require.Error(t, err)
				require.Nil(t, sum)
				require.Nil(t, dst.data)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.size, dst.size)
				require.Equal(t, tc.data, string(dst.data))
				require.Equal(t, "gc/new", dst.name)
				require.Equal(t, "gc/old", src.readName)
				want := sha256.Sum256([]byte(tc.data))
				require.Equal(t, want[:], sum)
			}
		})
	}
	for _, phase := range []string{"stat", "read", "write", "stream", "provider_copy"} {
		t.Run(phase+"_failure", func(t *testing.T) {
			src := &backupSizedSource{data: "a", size: 1}
			dst := &backupSizedTarget{}
			switch phase {
			case "stat":
				src.statErr = context.Canceled
			case "read":
				src.readErr = context.Canceled
			case "write":
				dst.writeErr = context.Canceled
			case "stream":
				src.streamErr = true
			case "provider_copy":
				dst.copyErr = context.Canceled
			}
			sum, err := (backupFileCopier{statSource: true}).copyFileWithRetry(t.Context(), src, dst, "object", "")
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, sum)
			require.False(t, src.readOpened)
			require.LessOrEqual(t, src.closes, 1)
			if phase == "stat" || phase == "provider_copy" {
				require.Zero(t, src.reads)
				require.Zero(t, dst.writes)
			}
		})
	}
	t.Run("public_copy_preserves_unknown_size", func(t *testing.T) {
		src := &backupSizedSource{data: "content", size: 1, statErr: errors.New("must not stat")}
		dst := &backupSizedTarget{}
		_, err := CopyFileWithRetry(t.Context(), src, dst, "object", "")
		require.NoError(t, err)
		require.Zero(t, src.stats)
		require.EqualValues(t, -1, dst.size)
		require.Equal(t, "content", string(dst.data))
		require.Equal(t, 1, src.closes)
	})
	t.Run("provider_copy_does_not_stat_or_read_source", func(t *testing.T) {
		src := &backupSizedSource{statErr: errors.New("must not stat")}
		dst := &flakyChecksumCopyFS{FileService: newBackupMemoryFS(t, "sized-provider"), data: []byte("data")}
		sum, err := (backupFileCopier{statSource: true}).copyFileWithRetry(t.Context(), src, dst, "object", "")
		require.NoError(t, err)
		want := sha256.Sum256(dst.data)
		require.Equal(t, want[:], sum)
		require.Zero(t, src.stats)
		require.Zero(t, src.reads)
		require.EqualValues(t, 1, dst.copies.Load())
		require.EqualValues(t, 2, dst.reads.Load())
	})
}

func TestBackupSizedCopyLogicalLocalSize(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw_%t", raw), func(t *testing.T) {
			ctx := t.Context()
			root := t.TempDir()
			newFS := fileservice.NewLocalFS
			if raw {
				newFS = fileservice.NewLocalFS2
			}
			src, err := newFS(ctx, "sized-local", root, fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			defer src.Close(ctx)
			data := []byte(strings.Repeat("logical contents", 512))
			require.NoError(t, writeFile(ctx, src, "ckp/object", data))
			physical, err := os.Stat(filepath.Join(root, "ckp/object"))
			require.NoError(t, err)
			if raw {
				require.EqualValues(t, len(data), physical.Size())
			} else {
				require.Greater(t, physical.Size(), int64(len(data)))
			}
			dst := &backupSizedTarget{}
			_, err = (backupFileCopier{statSource: true}).copyFile(ctx, fileservice.SubPath(src, "ckp"), dst, "object", "")
			require.NoError(t, err)
			require.EqualValues(t, len(data), dst.size)
			require.Equal(t, data, dst.data)
		})
	}
}

// emptyContentSHA256Hex is the well-known FIPS 180-4 SHA-256 digest of the
// empty input, used as an independent oracle instead of deriving it from the
// helper or the package's own hashing code.
const emptyContentSHA256Hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// TestChecksumFileReadEOFProvesEmptyObject drives the package-local
// checksumFile helper against read-open failures that surface EOF, like the
// MinIO SDK does for a zero-length object, and pins which outcomes may
// resolve to the empty-content checksum.
func TestChecksumFileReadEOFProvesEmptyObject(t *testing.T) {
	t.Run("verified_empty", func(t *testing.T) {
		src := &backupSizedSource{size: 0, readErr: io.EOF}
		sum, err := checksumFile(t.Context(), src, "object")
		require.NoError(t, err)
		want, err := hex.DecodeString(emptyContentSHA256Hex)
		require.NoError(t, err)
		require.Equal(t, want, sum)
		require.Equal(t, 1, src.reads)
		require.Equal(t, 1, src.stats)
		require.Equal(t, "object", src.statName)
	})
	t.Run("non_empty_keeps_read_failure", func(t *testing.T) {
		src := &backupSizedSource{size: 4, readErr: io.EOF}
		sum, err := checksumFile(t.Context(), src, "object")
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, sum)
		require.Equal(t, 1, src.stats)
	})
	t.Run("negative_size_keeps_read_failure", func(t *testing.T) {
		src := &backupSizedSource{size: -1, readErr: io.EOF}
		sum, err := checksumFile(t.Context(), src, "object")
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, sum)
		require.Equal(t, 1, src.stats)
	})
	t.Run("missing_object_surfaces_stat_error", func(t *testing.T) {
		src := &backupSizedSource{
			size:    0,
			readErr: io.EOF,
			statErr: moerr.NewFileNotFoundNoCtx("object"),
		}
		sum, err := checksumFile(t.Context(), src, "object")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
		require.Nil(t, sum)
	})
	t.Run("stat_error_surfaces", func(t *testing.T) {
		statErr := errors.New("head not answerable")
		src := &backupSizedSource{size: 0, readErr: io.EOF, statErr: statErr}
		sum, err := checksumFile(t.Context(), src, "object")
		require.ErrorIs(t, err, statErr)
		require.Nil(t, sum)
	})
	t.Run("stat_canceled_keeps_causality", func(t *testing.T) {
		src := &backupSizedSource{size: 0, readErr: io.EOF, statErr: context.Canceled}
		sum, err := checksumFile(t.Context(), src, "object")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, sum)
	})
	t.Run("canceled_ctx_with_size_zero_stat_is_not_green", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		// The stub ignores ctx, so a zero-size stat must still not bypass
		// the caller's cancellation causality.
		src := &backupSizedSource{size: 0, readErr: io.EOF}
		sum, err := checksumFile(ctx, src, "object")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, sum)
	})
	t.Run("read_canceled_is_not_probed", func(t *testing.T) {
		src := &backupSizedSource{size: 0, readErr: context.Canceled}
		sum, err := checksumFile(t.Context(), src, "object")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, sum)
		require.Zero(t, src.stats)
	})
	t.Run("read_eof_joined_with_cancel_keeps_causality", func(t *testing.T) {
		src := &backupSizedSource{size: 0, readErr: errors.Join(io.EOF, context.Canceled)}
		sum, err := checksumFile(t.Context(), src, "object")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, sum)
		// A cancellation carried by the read error keeps its cause without
		// probing for emptiness.
		require.Zero(t, src.stats)
	})
}

// backupEOFAttachReadFS hands over a reader even though Read fails, which
// callers must still close exactly once; implementations are free to do so.
type backupEOFAttachReadFS struct {
	fileservice.FileService
	name      string
	entrySize int64
	readErr   error
	reader    io.ReadCloser
	onStat    func()
	reads     int
	stats     int
}

func (f *backupEOFAttachReadFS) Read(_ context.Context, vector *fileservice.IOVector) error {
	f.reads++
	if f.reader != nil {
		*vector.Entries[0].ReadCloserForRead = f.reader
	}
	return f.readErr
}

func (f *backupEOFAttachReadFS) StatFile(context.Context, string) (*fileservice.DirEntry, error) {
	f.stats++
	if f.onStat != nil {
		f.onStat()
	}
	return &fileservice.DirEntry{Name: f.name, Size: f.entrySize}, nil
}

type backupCloseCountingReader struct {
	closeErr error
	closes   int
}

func (backupCloseCountingReader) Read([]byte) (int, error) { return 0, io.EOF }

func (c *backupCloseCountingReader) Close() error {
	c.closes++
	return c.closeErr
}

func TestChecksumFileReadEOFAttachedReader(t *testing.T) {
	t.Run("close_error_is_not_verified", func(t *testing.T) {
		rc := &backupCloseCountingReader{closeErr: errors.New("close boom")}
		src := &backupEOFAttachReadFS{
			name: "object", entrySize: 0, readErr: io.EOF, reader: rc,
		}
		sum, err := checksumFile(t.Context(), src, "object")
		require.ErrorContains(t, err, "close boom")
		require.Nil(t, sum)
		require.Equal(t, 1, rc.closes)
		require.Equal(t, 1, src.reads)
		require.Zero(t, src.stats)
	})
	t.Run("closed_once_then_proved_empty", func(t *testing.T) {
		rc := &backupCloseCountingReader{}
		src := &backupEOFAttachReadFS{
			name: "object", entrySize: 0, readErr: io.EOF, reader: rc,
		}
		sum, err := checksumFile(t.Context(), src, "object")
		require.NoError(t, err)
		want, err := hex.DecodeString(emptyContentSHA256Hex)
		require.NoError(t, err)
		require.Equal(t, want, sum)
		require.Equal(t, 1, rc.closes)
		require.Equal(t, 1, src.stats)
	})
	t.Run("cancel_keeps_causality_over_close_error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		rc := &backupCloseCountingReader{closeErr: errors.New("close boom")}
		src := &backupEOFAttachReadFS{
			name: "object", entrySize: 0, readErr: io.EOF, reader: rc,
		}
		sum, err := checksumFile(ctx, src, "object")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, sum)
		require.Equal(t, 1, rc.closes)
	})
	t.Run("non_empty_target_keeps_read_failure", func(t *testing.T) {
		rc := &backupCloseCountingReader{}
		src := &backupEOFAttachReadFS{
			name: "object", entrySize: 7, readErr: io.EOF, reader: rc,
		}
		sum, err := checksumFile(t.Context(), src, "object")
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, sum)
		require.Equal(t, 1, rc.closes)
		require.Equal(t, 1, src.stats)
	})
}

// TestChecksumFileReadEOFCausePrecedence pins the two root-reviewed cause
// rules for a read-EOF target: a cancellation carried by the read error must
// survive a later close failure instead of being replaced by it, and a
// cancellation observed during the stat must be returned before any
// substitute size-based read failure.
func TestChecksumFileReadEOFCausePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		terminal error
		closeErr error
	}{
		{"canceled_clean_close", context.Canceled, nil},
		{"canceled_close_failure", context.Canceled, errors.New("close boom")},
		{"deadline_clean_close", context.DeadlineExceeded, nil},
		{"deadline_close_failure", context.DeadlineExceeded, errors.New("close boom")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rc := &backupCloseCountingReader{closeErr: tc.closeErr}
			src := &backupEOFAttachReadFS{
				name: "object", entrySize: 0,
				readErr: errors.Join(io.EOF, tc.terminal), reader: rc,
			}
			sum, err := checksumFile(t.Context(), src, "object")
			require.ErrorIs(t, err, tc.terminal)
			require.Nil(t, sum)
			require.Equal(t, 1, rc.closes)
			require.Zero(t, src.stats)
			if tc.closeErr != nil {
				// The close failure is recorded without replacing the read
				// cancellation cause.
				require.ErrorContains(t, err, "close boom")
			}
		})
	}
	for _, tc := range []struct {
		name string
		size int64
	}{
		{"nonzero_size", 7},
		{"negative_size", -1},
	} {
		t.Run("stat_cancel_"+tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			src := &backupEOFAttachReadFS{
				name: "object", entrySize: tc.size, readErr: io.EOF,
			}
			src.onStat = cancel
			sum, err := checksumFile(ctx, src, "object")
			require.ErrorIs(t, err, context.Canceled)
			require.NotErrorIs(t, err, io.EOF)
			require.Nil(t, sum)
			require.Equal(t, 1, src.stats)
		})
	}
}
