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

package logservice

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestRecoverWALDataNoopAndErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("disabled", func(t *testing.T) {
		require.NoError(t, (&Service{}).RecoverWALData(ctx, Config{}))
	})

	t.Run("missing local file service", func(t *testing.T) {
		s := &Service{runtime: runtime.DefaultRuntime()}
		cfg := Config{}
		cfg.BootstrapConfig.Restore.WALDataPath = filepath.Join(t.TempDir(), "wal_data.bin")
		require.Error(t, s.RecoverWALData(ctx, cfg))
	})

	t.Run("malformed WAL", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal_data.bin")
		require.NoError(t, os.WriteFile(path, []byte{1, 2, 3}, 0644))
		s := &Service{runtime: runtime.DefaultRuntime(), fileService: newFS()}
		cfg := DefaultConfig()
		cfg.BootstrapConfig.Restore.WALDataPath = path
		require.ErrorContains(t, s.RecoverWALData(ctx, cfg), "too small")
	})
}

func TestRecoverWALDataDoesNotTrustLegacyCompletionTag(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "missing-wal-data.bin")
	fileServices := newFS()
	local, err := fileservice.Get[fileservice.FileService](fileServices, defines.LocalFileServiceName)
	require.NoError(t, err)
	tag := getWALRecoveredTagFile(path)
	require.NoError(t, local.Write(ctx, fileservice.IOVector{
		FilePath: tag,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   1,
			Data:   []byte{1},
		}},
	}))

	s := &Service{runtime: runtime.DefaultRuntime(), fileService: fileServices}
	cfg := DefaultConfig()
	cfg.BootstrapConfig.Restore.WALDataPath = path
	cfg.BootstrapConfig.Restore.Force = true
	require.ErrorContains(t, s.RecoverWALData(ctx, cfg), "failed to open WAL data file")
	_, err = os.Stat(getWALRecoveryStateFile(path))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestWaitWALRecoveryInterval(t *testing.T) {
	require.NoError(t, waitWALRecoveryInterval(context.Background(), time.Millisecond))
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(context.Canceled)
	require.ErrorIs(t, waitWALRecoveryInterval(ctx, time.Hour), context.Canceled)
}

func TestReadWALDataFileOrdersByRaftIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal_data.bin")
	input := []WALEntry{
		{DSN: 1, SafeDSN: 10, RaftIndex: 300, RaftTerm: 1, EntryCount: 1, RawData: testRawLogEntry(1, 10, 1)},
		{DSN: 2, SafeDSN: 20, RaftIndex: 100, RaftTerm: 1, EntryCount: 1, RawData: testRawLogEntry(2, 20, 1)},
		{DSN: 3, SafeDSN: 30, RaftIndex: 200, RaftTerm: 1, EntryCount: 1, RawData: testRawLogEntry(3, 30, 1)},
	}
	if err := writeTestWALDataFile(path, input); err != nil {
		t.Fatal(err)
	}

	s := &Service{runtime: runtime.DefaultRuntime()}
	data, err := s.readWALDataFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if data.Digest == "" {
		t.Fatal("expected WAL content digest")
	}
	require.Equal(t, path, data.Path)
	for _, entry := range data.Entries {
		require.Nil(t, entry.RawData, "file-backed WAL payloads must not be retained in memory")
		require.Positive(t, entry.dataLen)
	}

	gotRaftIndexes := make([]uint64, 0, len(data.Entries))
	gotDSNs := make([]uint64, 0, len(data.Entries))
	for _, entry := range data.Entries {
		gotRaftIndexes = append(gotRaftIndexes, entry.RaftIndex)
		gotDSNs = append(gotDSNs, entry.DSN)
	}

	if want := []uint64{100, 200, 300}; !reflect.DeepEqual(gotRaftIndexes, want) {
		t.Fatalf("unexpected raft index order: got %v, want %v", gotRaftIndexes, want)
	}
	if want := []uint64{2, 3, 1}; !reflect.DeepEqual(gotDSNs, want) {
		t.Fatalf("unexpected DSN order: got %v, want %v", gotDSNs, want)
	}
}

func TestReadWALDataFileUsesConfiguredRPCPayloadLimit(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.RPC.MaxMessageSize = 70 * 1024
	limit, err := walRecoveryMaxEntryDataSize(ctx, cfg)
	require.NoError(t, err)
	require.Less(t, limit, uint32(70*1024))

	path := filepath.Join(t.TempDir(), "wal_data.bin")
	require.NoError(t, writeTestWALDataFile(path, []WALEntry{{
		DSN:        1,
		RaftIndex:  1,
		EntryCount: 1,
		RawData:    make([]byte, 70*1024),
	}}))
	s := &Service{runtime: runtime.DefaultRuntime()}
	_, err = s.readWALDataFileWithLimit(ctx, path, limit)
	require.ErrorContains(t, err, "configured RPC payload limit")

	cfg.RPC.MaxMessageSize = 1
	_, err = walRecoveryMaxEntryDataSize(ctx, cfg)
	require.ErrorContains(t, err, "too small")
}

func TestReadWALDataFileNormalizesSafeDSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal_data.bin")
	input := []WALEntry{
		{DSN: 10, SafeDSN: 60, RaftIndex: 100, RaftTerm: 1, EntryCount: 11, RawData: testRawLogEntry(10, 60, 11)},
		{DSN: 50, SafeDSN: 60, RaftIndex: 200, RaftTerm: 1, EntryCount: 11, RawData: testRawLogEntry(50, 60, 11)},
		{DSN: 21, SafeDSN: 60, RaftIndex: 300, RaftTerm: 1, EntryCount: 29, RawData: testRawLogEntry(21, 60, 29)},
	}
	if err := writeTestWALDataFile(path, input); err != nil {
		t.Fatal(err)
	}

	s := &Service{runtime: runtime.DefaultRuntime()}
	data, err := s.readWALDataFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}

	gotSafeDSNs := make([]uint64, 0, len(data.Entries))
	gotPayloadSafeDSNs := make([]uint64, 0, len(data.Entries))
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	for i, entry := range data.Entries {
		gotSafeDSNs = append(gotSafeDSNs, entry.SafeDSN)
		payload, err := loadWALRecoveryEntryData(context.Background(), data, file, i)
		require.NoError(t, err)
		gotPayloadSafeDSNs = append(
			gotPayloadSafeDSNs,
			binary.LittleEndian.Uint64(payload[logEntrySafeDSNOffset:]),
		)
	}

	if want := []uint64{9, 20, 20}; !reflect.DeepEqual(gotSafeDSNs, want) {
		t.Fatalf("unexpected safe DSNs: got %v, want %v", gotSafeDSNs, want)
	}
	if want := []uint64{9, 20, 20}; !reflect.DeepEqual(gotPayloadSafeDSNs, want) {
		t.Fatalf("unexpected payload safe DSNs: got %v, want %v", gotPayloadSafeDSNs, want)
	}
}

func TestReadWALDataFilePreservesSourceSafeDSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal_data.bin")
	input := []WALEntry{
		{DSN: 10, SafeDSN: 3, RaftIndex: 100, RaftTerm: 1, EntryCount: 1, RawData: testRawLogEntry(10, 3, 1)},
		{DSN: 4, SafeDSN: 3, RaftIndex: 200, RaftTerm: 1, EntryCount: 1, RawData: testRawLogEntry(4, 3, 1)},
		{DSN: 5, SafeDSN: 4, RaftIndex: 300, RaftTerm: 1, EntryCount: 5, RawData: testRawLogEntry(5, 4, 5)},
	}
	require.NoError(t, writeTestWALDataFile(path, input))

	s := &Service{runtime: runtime.DefaultRuntime()}
	data, err := s.readWALDataFile(context.Background(), path)
	require.NoError(t, err)

	got := make([]uint64, 0, len(data.Entries))
	gotPayload := make([]uint64, 0, len(data.Entries))
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	for i, entry := range data.Entries {
		got = append(got, entry.SafeDSN)
		payload, err := loadWALRecoveryEntryData(context.Background(), data, file, i)
		require.NoError(t, err)
		gotPayload = append(gotPayload,
			binary.LittleEndian.Uint64(payload[logEntrySafeDSNOffset:]))
	}
	require.Equal(t, []uint64{3, 3, 4}, got)
	require.Equal(t, []uint64{3, 3, 4}, gotPayload)
}

func TestValidateWALLogEntry(t *testing.T) {
	ctx := context.Background()
	outer := WALEntry{DSN: 10, SafeDSN: 9, EntryCount: 2}
	valid := testRawLogEntry(outer.DSN, outer.SafeDSN, outer.EntryCount)
	require.NoError(t, validateWALLogEntry(ctx, 0, outer, valid))

	skip := testRawSkipLogEntry()
	require.NoError(t, validateWALLogEntry(ctx, 0,
		WALEntry{DSN: 0, EntryCount: 1}, skip))

	tests := []struct {
		name   string
		outer  WALEntry
		mutate func([]byte)
		want   string
	}{
		{
			name:  "too small",
			outer: outer,
			mutate: func(data []byte) {
				clear(data[:])
			},
			want: "invalid LogEntry type",
		},
		{
			name:  "wrong type",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint16(data[logEntryTypeOffset:], 999)
			},
			want: "invalid LogEntry type",
		},
		{
			name:  "wrong version",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint16(data[logEntryVersionOffset:], 1)
			},
			want: "unsupported LogEntry version",
		},
		{
			name:  "wrong command",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint16(data[logEntryCommandOffset:], 9)
			},
			want: "invalid LogEntry command",
		},
		{
			name:  "DSN mismatch",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint64(data[logEntryStartDSNOffset:], 11)
			},
			want: "metadata mismatch",
		},
		{
			name:  "count mismatch",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint32(data[logEntryCountOffset:], 1)
			},
			want: "metadata mismatch",
		},
		{
			name:  "invalid footer offset",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint32(data[logEntryFooterOffset:], logEntryHeaderSize-1)
			},
			want: "invalid footer offset",
		},
		{
			name:  "footer size mismatch",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint32(data[logEntryFooterOffset:], logEntryHeaderSize)
			},
			want: "footer size does not match",
		},
		{
			name:  "footer record gap",
			outer: outer,
			mutate: func(data []byte) {
				footer := binary.LittleEndian.Uint32(data[logEntryFooterOffset:])
				binary.LittleEndian.PutUint32(data[footer:], logEntryHeaderSize+1)
			},
			want: "invalid footer record",
		},
		{
			name:  "zero footer record length",
			outer: outer,
			mutate: func(data []byte) {
				footer := binary.LittleEndian.Uint32(data[logEntryFooterOffset:])
				binary.LittleEndian.PutUint32(data[footer+4:], 0)
			},
			want: "invalid footer record",
		},
		{
			name:  "embedded TN DSN mismatch",
			outer: outer,
			mutate: func(data []byte) {
				footer := binary.LittleEndian.Uint32(data[logEntryFooterOffset:])
				secondEntryOffset := binary.LittleEndian.Uint32(
					data[footer+logEntryFooterRecordSize:],
				)
				binary.LittleEndian.PutUint64(data[secondEntryOffset:], outer.DSN+99)
			},
			want: "embedded DSN",
		},
		{
			name:  "invalid TN entry type",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint16(
					data[logEntryHeaderSize+tnEntryTypeOffset:], tnEntryMinType-1)
			},
			want: "unsupported type/version",
		},
		{
			name:  "invalid TN entry sizes",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint32(
					data[logEntryHeaderSize+tnEntryPayloadSizeOffset:], math.MaxUint32)
			},
			want: "invalid info/payload sizes",
		},
		{
			name:  "malformed TN info",
			outer: outer,
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint64(
					data[logEntryHeaderSize+tnEntryInfoOffset+20:], 1)
			},
			want: "malformed info metadata",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			tc.mutate(data)
			err := validateWALLogEntry(ctx, 0, tc.outer, data)
			require.ErrorContains(t, err, tc.want)
		})
	}

	require.ErrorContains(t,
		validateWALLogEntry(ctx, 0, outer, valid[:logEntryHeaderSize-1]),
		"payload is too small")
}

func TestValidateWALTNEntryInfoBounds(t *testing.T) {
	ctx := context.Background()
	setUint64 := func(data []byte, offset int, value uint64) {
		binary.LittleEndian.PutUint64(data[offset:], value)
	}
	setUint32 := func(data []byte, offset int, value uint32) {
		binary.LittleEndian.PutUint32(data[offset:], value)
	}

	tests := []struct {
		name string
		info []byte
	}{
		{
			name: "missing checkpoint count",
			info: make([]byte, 20),
		},
		{
			name: "checkpoint count exceeds remaining bytes",
			info: func() []byte {
				data := make([]byte, 28)
				setUint64(data, 20, 1)
				return data
			}(),
		},
		{
			name: "interval count exceeds remaining bytes",
			info: func() []byte {
				data := make([]byte, 48)
				setUint64(data, 20, 1)
				setUint64(data, 32, 1)
				return data
			}(),
		},
		{
			name: "command count exceeds remaining bytes",
			info: func() []byte {
				data := make([]byte, 48)
				setUint64(data, 20, 1)
				setUint64(data, 40, 1)
				return data
			}(),
		},
		{
			name: "command ID count exceeds remaining bytes",
			info: func() []byte {
				data := make([]byte, 64)
				setUint64(data, 20, 1)
				setUint64(data, 40, 1)
				setUint32(data, 56, math.MaxUint32)
				return data
			}(),
		},
		{
			name: "trailing metadata",
			info: make([]byte, 29),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t,
				validateWALTNEntryInfo(ctx, 1, 2, tc.info),
				"malformed info metadata")
		})
	}

	// Exercise every nested level with one checkpoint, interval, and command.
	// A zero command-ID count still carries the command size field.
	valid := make([]byte, 80)
	setUint64(valid, 20, 1) // checkpoint count
	setUint32(valid, 28, 7) // checkpoint group
	setUint64(valid, 32, 1) // interval count
	setUint64(valid, 40, 10)
	setUint64(valid, 48, 20)
	setUint64(valid, 56, 1) // command count
	setUint64(valid, 64, 15)
	setUint32(valid, 72, 0) // command ID count
	setUint32(valid, 76, 9) // command size
	require.NoError(t, validateWALTNEntryInfo(ctx, 1, 2, valid))
}

func TestWALRecoveryRejectsSourceMutationAfterScan(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal_data.bin")
	require.NoError(t, writeTestWALDataFile(path, []WALEntry{{
		DSN:        1,
		SafeDSN:    1,
		RaftIndex:  1,
		RaftTerm:   1,
		EntryCount: 1,
		RawData:    testRawLogEntry(1, 1, 1),
	}}))

	s := &Service{runtime: runtime.DefaultRuntime()}
	walData, err := s.readWALDataFile(ctx, path)
	require.NoError(t, err)
	require.Len(t, walData.Entries, 1)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	bodyOffset := walData.Entries[0].dataOffset + logEntryHeaderSize
	_, err = f.WriteAt([]byte{0xff}, bodyOffset)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	f, err = os.Open(path)
	require.NoError(t, err)
	_, err = loadWALRecoveryEntryData(ctx, walData, f, 0)
	require.ErrorContains(t, err, "changed after validation")
	require.NoError(t, f.Close())
	require.ErrorContains(t,
		verifyWALRecoverySource(ctx, path, walData.Digest),
		"source changed during replay")
}

func TestReadWALDataFileRejectsMalformedFiles(t *testing.T) {
	s := &Service{runtime: runtime.DefaultRuntime()}
	testCases := []struct {
		name       string
		writeFile  func(string) error
		wantErrMsg string
	}{
		{
			name: "too small",
			writeFile: func(path string) error {
				return os.WriteFile(path, []byte{1, 2, 3}, 0644)
			},
			wantErrMsg: "too small",
		},
		{
			name: "count exceeds limit",
			writeFile: func(path string) error {
				data := make([]byte, walDataFileCountSize)
				binary.LittleEndian.PutUint32(data, uint32(maxWALRecoveryEntries+1))
				return os.WriteFile(path, data, 0644)
			},
			wantErrMsg: "exceeds limit",
		},
		{
			name: "file exceeds memory limit",
			writeFile: func(path string) error {
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				if err := f.Truncate(maxWALRecoveryFileSize + 1); err != nil {
					_ = f.Close()
					return err
				}
				return f.Close()
			},
			wantErrMsg: "exceeds recovery limit",
		},
		{
			name: "count exceeds file size",
			writeFile: func(path string) error {
				data := make([]byte, walDataFileCountSize)
				binary.LittleEndian.PutUint32(data, 1)
				return os.WriteFile(path, data, 0644)
			},
			wantErrMsg: "exceeds max possible entries",
		},
		{
			name: "trailing bytes",
			writeFile: func(path string) error {
				data := make([]byte, walDataFileCountSize+1)
				return os.WriteFile(path, data, 0644)
			},
			wantErrMsg: "trailing bytes",
		},
		{
			name: "data length exceeds limit",
			writeFile: func(path string) error {
				data := make([]byte, walDataFileCountSize+walDataFileEntryHeaderSize)
				binary.LittleEndian.PutUint32(data[:walDataFileCountSize], 1)
				binary.LittleEndian.PutUint32(data[walDataFileCountSize+36:], uint32(maxWALRecoveryEntryDataSize+1))
				return os.WriteFile(path, data, 0644)
			},
			wantErrMsg: "exceeds configured RPC payload limit",
		},
		{
			name: "data length exceeds remaining bytes",
			writeFile: func(path string) error {
				data := make([]byte, walDataFileCountSize+walDataFileEntryHeaderSize)
				binary.LittleEndian.PutUint32(data[:walDataFileCountSize], 1)
				binary.LittleEndian.PutUint32(data[walDataFileCountSize+36:], 8)
				return os.WriteFile(path, data, 0644)
			},
			wantErrMsg: "exceeds remaining file bytes",
		},
		{
			name: "duplicate raft index",
			writeFile: func(path string) error {
				return writeTestWALDataFile(path, []WALEntry{
					{DSN: 1, RaftIndex: 10, EntryCount: 1},
					{DSN: 2, RaftIndex: 10, EntryCount: 1},
				})
			},
			wantErrMsg: "raft indexes are not unique",
		},
		{
			name: "zero raft index",
			writeFile: func(path string) error {
				return writeTestWALDataFile(path, []WALEntry{
					{DSN: 1, RaftIndex: 0, EntryCount: 1},
				})
			},
			wantErrMsg: "invalid zero raft index",
		},
		{
			name: "DSN range overflow",
			writeFile: func(path string) error {
				return writeTestWALDataFile(path, []WALEntry{
					{DSN: ^uint64(0), RaftIndex: 10, EntryCount: 2},
				})
			},
			wantErrMsg: "DSN range overflows",
		},
		{
			name: "skip references exceed WAL entry count",
			writeFile: func(path string) error {
				return writeTestWALDataFile(path, []WALEntry{{
					RaftIndex:  10,
					EntryCount: 1,
					RawData: testRawSkipLogEntryWithPairs(
						[]uint64{1, 2}, []uint64{1, 2}),
				}})
			},
			wantErrMsg: "skip reference count 2 exceeds WAL entry count 1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal_data.bin")
			if err := tc.writeFile(path); err != nil {
				t.Fatal(err)
			}
			_, err := s.readWALDataFile(context.Background(), path)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.wantErrMsg) {
				t.Fatalf("unexpected error: got %v, want containing %q", err, tc.wantErrMsg)
			}
		})
	}
}

func TestReadWALDataFileAcceptsMultiReferenceSkip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal_data.bin")
	require.NoError(t, writeTestWALDataFile(path, []WALEntry{
		{
			DSN:        1,
			RaftIndex:  1,
			EntryCount: 1,
			RawData:    testRawLogEntry(1, 0, 1),
		},
		{
			DSN:        2,
			RaftIndex:  2,
			EntryCount: 1,
			RawData:    testRawLogEntry(2, 0, 1),
		},
		{
			RaftIndex:  3,
			EntryCount: 1,
			RawData: testRawSkipLogEntryWithPairs(
				[]uint64{1, 2}, []uint64{1, 2}),
		},
	}))

	s := &Service{runtime: runtime.DefaultRuntime()}
	walData, err := s.readWALDataFile(ctx, path)
	require.NoError(t, err)
	require.Len(t, walData.Entries, 3)
}

func TestReadWALDataFileAcceptsMultiGiBInputLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal_data.bin")
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Truncate((2<<30)+1))
	require.NoError(t, f.Close())

	s := &Service{runtime: runtime.DefaultRuntime()}
	_, err = s.readWALDataFile(context.Background(), path)
	require.ErrorContains(t, err, "trailing bytes")
	require.NotContains(t, err.Error(), "exceeds recovery limit")
}

func TestBuildUserEntryCmdUsesLogserviceCommandLayout(t *testing.T) {
	payload := []byte{1, 2, 3}
	cmd := buildUserEntryCmd(42, payload)

	if got := parseCmdTag(cmd); got != pb.UserEntryUpdate {
		t.Fatalf("unexpected command tag: got %v, want %v", got, pb.UserEntryUpdate)
	}
	if got := parseLeaseHolderID(cmd); got != 42 {
		t.Fatalf("unexpected lease holder ID: got %d, want 42", got)
	}
	if !bytes.Equal(cmd[headerSize+leaseHolderIDSize:], payload) {
		t.Fatalf("unexpected payload: got %v, want %v", cmd[headerSize+leaseHolderIDSize:], payload)
	}
}

func TestWALRecoveryHelperBoundaries(t *testing.T) {
	require.Equal(t, maxWALRecoveryPreallocEntries,
		walRecoveryPreallocEntries(maxWALRecoveryPreallocEntries+1))
	require.Equal(t, 7, walRecoveryPreallocEntries(7))
	require.False(t, shouldCheckpointWALRecoveryState(1, 1000))
	require.True(t, shouldCheckpointWALRecoveryState(walRecoveryCheckpointEntries, 1000))
	require.True(t, shouldCheckpointWALRecoveryState(7, 7))

	intervals := dsnIntervalHeap{
		{start: 2, end: 4},
		{start: 1, end: 5},
		{start: 2, end: 3},
	}
	require.False(t, intervals.Less(0, 1))
	require.False(t, intervals.Less(0, 2))
	require.True(t, intervals.Less(2, 0))

	entry := WALEntry{RawData: []byte("payload")}
	data := &WALRecoveryData{Entries: []WALEntry{entry}}
	matches, err := walRecoveryRecordMatches(
		context.Background(), data, nil, 0, pb.LogRecord{Type: pb.Internal})
	require.NoError(t, err)
	require.False(t, matches)
	matches, err = walRecoveryRecordMatches(context.Background(), data, nil, 0, pb.LogRecord{
		Type: pb.UserRecord,
		Data: make([]byte, headerSize+leaseHolderIDSize),
	})
	require.NoError(t, err)
	require.False(t, matches)

	_, err = loadWALRecoveryEntryData(context.Background(), data, nil, 1)
	require.ErrorContains(t, err, "out of range")
	fileBacked := &WALRecoveryData{Entries: []WALEntry{{dataLen: 1}}}
	_, err = loadWALRecoveryEntryData(context.Background(), fileBacked, nil, 0)
	require.ErrorContains(t, err, "not open")
	path := filepath.Join(t.TempDir(), "short.bin")
	require.NoError(t, os.WriteFile(path, nil, 0644))
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	_, err = loadWALRecoveryEntryData(context.Background(), fileBacked, file, 0)
	require.ErrorContains(t, err, "failed to read")
}

func TestGetWALRecoveredTagFileUsesWALDataPath(t *testing.T) {
	first := getWALRecoveredTagFile(filepath.Join(t.TempDir(), "wal_data.bin"))
	second := getWALRecoveredTagFile(filepath.Join(t.TempDir(), "wal_data.bin"))

	if first == walRecoveredTagFilePrefix {
		t.Fatalf("unexpected generic WAL recovery tag path: %s", first)
	}
	if first == second {
		t.Fatalf("expected different WAL paths to use different tag files: %s", first)
	}
	if !strings.HasPrefix(first, walRecoveredTagFilePrefix+"_") {
		t.Fatalf("unexpected WAL recovery tag path: %s", first)
	}
}

func TestWALRecoveryProgressAfterRecords(t *testing.T) {
	ctx := context.Background()
	data := &WALRecoveryData{
		Entries: []WALEntry{
			{RawData: []byte("entry-1")},
			{RawData: []byte("entry-2")},
			{RawData: []byte("entry-3")},
		},
		Digest: "digest-a",
	}
	state := &walRecoveryState{
		Version:          walRecoveryStateVersion,
		WALDigest:        data.Digest,
		EntryCount:       3,
		BaseLSN:          0,
		CompletedEntries: 0,
		LastLSN:          0,
	}

	records := []pb.LogRecord{
		{Lsn: 1, Type: pb.Internal},
		{Lsn: 2, Type: pb.Internal},
		{Lsn: 3, Type: pb.LeaseUpdate},
		{Lsn: 4, Type: pb.LeaseRejected},
		{Lsn: 5, Type: pb.UserRecord, Data: buildUserEntryCmd(42, data.Entries[0].RawData)},
		{Lsn: 6, Type: pb.Internal},
		{Lsn: 8, Type: pb.LeaseUpdate},
		{Lsn: 9, Type: pb.UserRecord, Data: buildUserEntryCmd(42, data.Entries[1].RawData)},
	}
	completed, err := walRecoveryProgressAfterRecords(ctx, state, data, nil, 9, records)
	if err != nil || completed != 2 {
		t.Fatalf("unexpected resume result: completed=%d err=%v", completed, err)
	}
	state.CompletedEntries = 1
	state.LastLSN = 5
	completed, err = walRecoveryProgressAfterRecords(ctx, state, data, nil, 9, records[5:])
	if err != nil || completed != 2 {
		t.Fatalf("unexpected persisted resume result: completed=%d err=%v", completed, err)
	}

	mismatch := []pb.LogRecord{
		{Lsn: 9, Type: pb.UserRecord, Data: buildUserEntryCmd(42, []byte("other-writer"))},
	}
	if _, err := walRecoveryProgressAfterRecords(ctx, state, data, nil, 9, mismatch); err == nil {
		t.Fatal("expected another writer detection")
	}

	state.CompletedEntries = 3
	state.LastLSN = 12
	state.Complete = true
	completed, err = walRecoveryProgressAfterRecords(ctx, state, data, nil, 20, nil)
	if err != nil || completed != uint64(len(data.Entries)) {
		t.Fatalf("completed recovery must allow later writes: completed=%d err=%v", completed, err)
	}
	if _, err := walRecoveryProgressAfterRecords(ctx, state, data, nil, 11, nil); err == nil {
		t.Fatal("expected completed recovery truncation detection")
	}

	state.WALDigest = "digest-b"
	if _, err := walRecoveryProgressAfterRecords(ctx, state, data, nil, 20, nil); err == nil {
		t.Fatal("expected WAL digest mismatch")
	}

	state.WALDigest = data.Digest
	state.Complete = false
	state.LastLSN = 12
	_, _, err = (&Service{}).reconcileWALRecoveryProgress(
		ctx, nil, state, data, nil, 11, 1024)
	require.ErrorContains(t, err, "below WAL recovery last LSN")
}

func TestWALRecoveryProgressRejectsUnexpectedExtraRecord(t *testing.T) {
	ctx := context.Background()
	data := &WALRecoveryData{
		Entries: []WALEntry{{RawData: []byte("only-entry")}},
		Digest:  "digest",
	}
	state := &walRecoveryState{
		Version:          walRecoveryStateVersion,
		WALDigest:        data.Digest,
		EntryCount:       1,
		CompletedEntries: 1,
		LastLSN:          5,
	}
	records := []pb.LogRecord{
		{Lsn: 6, Type: pb.Internal},
		{Lsn: 7, Type: pb.LeaseUpdate},
		{Lsn: 8, Type: pb.LeaseRejected},
		{Lsn: 6, Type: pb.UserRecord, Data: buildUserEntryCmd(42, []byte("unexpected"))},
	}
	if _, err := walRecoveryProgressAfterRecords(ctx, state, data, nil, 8, records[:3]); err != nil {
		t.Fatalf("expected non-user records after recovered WAL to be accepted: %v", err)
	}
	records[3].Lsn = 9
	if _, err := walRecoveryProgressAfterRecords(ctx, state, data, nil, 9, records); err == nil {
		t.Fatal("expected extra destination record to be rejected")
	}
}

func TestWALRecoveryStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal_data.bin"+walRecoveryStateSuffix)
	want := walRecoveryState{
		Version:          walRecoveryStateVersion,
		WALDigest:        "abc123",
		EntryCount:       42,
		BaseLSN:          3,
		CompletedEntries: 42,
		LastLSN:          50,
		Complete:         true,
	}
	if err := writeWALRecoveryState(ctx, path, want); err != nil {
		t.Fatal(err)
	}
	got, err := readWALRecoveryState(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("unexpected state: got %+v, want %+v", *got, want)
	}

	legacy := []byte(`{"version":1,"wal_digest":"abc123","entry_count":42,"base_lsn":3,"complete":true}`)
	if err := os.WriteFile(path, legacy, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readWALRecoveryState(ctx, path); err == nil {
		t.Fatal("expected legacy recovery state to be rejected")
	}

	if err := os.WriteFile(path, []byte("{"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readWALRecoveryState(ctx, path); err == nil {
		t.Fatal("expected malformed recovery state to be rejected")
	}
}

func TestValidateWALRecoveryStateRejectsCorruption(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name       string
		state      walRecoveryState
		entryCount uint64
		want       string
	}{
		{
			name:       "entry count changed",
			state:      walRecoveryState{WALDigest: "digest", EntryCount: 2},
			entryCount: 1,
			want:       "entry count changed",
		},
		{
			name: "completed entries exceed source",
			state: walRecoveryState{
				WALDigest: "digest", EntryCount: 1, CompletedEntries: 2,
			},
			entryCount: 1,
			want:       "more than source entry count",
		},
		{
			name: "last LSN below base",
			state: walRecoveryState{
				WALDigest: "digest", EntryCount: 1, BaseLSN: 2, LastLSN: 1,
			},
			entryCount: 1,
			want:       "below base LSN",
		},
		{
			name: "incomplete entries marked complete",
			state: walRecoveryState{
				WALDigest: "digest", EntryCount: 2, CompletedEntries: 1, Complete: true,
			},
			entryCount: 2,
			want:       "completed WAL recovery",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateWALRecoveryState(ctx, &tc.state, "digest", tc.entryCount)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestAddWALRecoveryStatusIsScopedToRecoveryMode(t *testing.T) {
	static := map[string]*pb.ConfigItem{
		"static": {Name: "static", CurrentValue: "value"},
	}
	hb := pb.LogStoreHeartbeat{ConfigData: &pb.ConfigData{Content: static}}
	service := &Service{}
	service.setWALRecoveryInProgress(true)
	if service.walRecovery.pending.Load() {
		t.Fatal("normal LogService unexpectedly entered WAL recovery")
	}
	service.addWALRecoveryStatus(&hb)
	if _, ok := hb.ConfigData.Content[walRecoveryStatusConfigKey]; ok {
		t.Fatal("normal LogService heartbeat unexpectedly contains WAL recovery status")
	}

	service.walRecovery.configured = true
	service.walRecovery.coordinator = true
	service.addWALRecoveryStatus(nil)
	service.walRecovery.pending.Store(true)
	service.addWALRecoveryStatus(&hb)
	if got := hb.ConfigData.Content[walRecoveryStatusConfigKey].CurrentValue; got != walRecoveryStatusCoordinatorPending {
		t.Fatalf("unexpected recovery status: got %q, want %q", got, walRecoveryStatusCoordinatorPending)
	}
	if _, ok := static[walRecoveryStatusConfigKey]; ok {
		t.Fatal("recovery status mutated the shared static config map")
	}

	service.setWALRecoveryInProgress(false)
	service.addWALRecoveryStatus(&hb)
	if got := hb.ConfigData.Content[walRecoveryStatusConfigKey].CurrentValue; got != walRecoveryStatusComplete {
		t.Fatalf("unexpected recovery status: got %q, want %q", got, walRecoveryStatusComplete)
	}
}

func TestWALRecoveryPendingUsesReplicatedStoreState(t *testing.T) {
	if walRecoveryPending(nil) {
		t.Fatal("nil checker state must not block bootstrap")
	}
	if !walRecoveryPending(&pb.CheckerState{LogServiceRecoveryPending: true}) {
		t.Fatal("expected replicated HAKeeper recovery state to block bootstrap")
	}
	state := &pb.CheckerState{
		LogState: pb.LogState{
			Stores: map[string]pb.LogStoreInfo{
				"remote-log-store": {
					ConfigData: &pb.ConfigData{Content: map[string]*pb.ConfigItem{
						walRecoveryStatusConfigKey: {
							CurrentValue: walRecoveryStatusPending,
						},
					}},
				},
			},
		},
	}
	if !walRecoveryPending(state) {
		t.Fatal("expected remote LogStore recovery status to block bootstrap")
	}
	state.LogServiceRecoveryCompleted = true
	if walRecoveryPending(state) {
		t.Fatal("durable recovery completion must override a stale LogStore heartbeat")
	}
	state.LogServiceRecoveryCompleted = false
	state.LogState.Stores["remote-log-store"].ConfigData.
		Content[walRecoveryStatusConfigKey].CurrentValue = walRecoveryStatusComplete
	if walRecoveryPending(state) {
		t.Fatal("completed remote recovery must not block bootstrap")
	}
	if walRecoveryPending(&pb.CheckerState{}) {
		t.Fatal("normal cluster without recovery status must not be blocked")
	}
}

func TestWALRecoveryCoordinator(t *testing.T) {
	recoveryStore := func(status string) pb.LogStoreInfo {
		return pb.LogStoreInfo{ConfigData: &pb.ConfigData{Content: map[string]*pb.ConfigItem{
			walRecoveryStatusConfigKey: {CurrentValue: status},
		}}}
	}
	state := &pb.CheckerState{
		LogState: pb.LogState{
			Shards: map[uint64]pb.LogShardInfo{
				firstLogShardID: {
					ShardID: firstLogShardID,
					Replicas: map[uint64]string{
						10: "store-b",
						11: "store-a",
						12: "store-c",
					},
				},
			},
			Stores: map[string]pb.LogStoreInfo{
				"store-a": recoveryStore(walRecoveryStatusCoordinatorPending),
				"store-b": recoveryStore(walRecoveryStatusPending),
			},
		},
	}
	if _, ready := walRecoveryCoordinator(nil, 3); ready {
		t.Fatal("nil state must not elect a coordinator")
	}
	if _, ready := walRecoveryCoordinator(state, 0); ready {
		t.Fatal("zero expected replicas must not elect a coordinator")
	}
	if _, ready := walRecoveryCoordinator(&pb.CheckerState{}, 3); ready {
		t.Fatal("missing Log shard must not elect a coordinator")
	}

	_, ready := walRecoveryCoordinator(state, 3)
	if ready {
		t.Fatal("all replica stores must report before election")
	}

	state.LogState.Stores["store-c"] = recoveryStore(walRecoveryStatusComplete)
	coordinator, ready := walRecoveryCoordinator(state, 3)
	if !ready || coordinator != "store-a" {
		t.Fatalf("unexpected coordinator: ready=%v, coordinator=%q", ready, coordinator)
	}

	state.LogState.Stores["store-a"] = recoveryStore(walRecoveryStatusComplete)
	if _, ready = walRecoveryCoordinator(state, 3); ready {
		t.Fatal("participants without the WAL file must not become coordinator")
	}

	state.LogState.Stores["store-b"] = pb.LogStoreInfo{}
	state.LogState.Stores["store-a"] = recoveryStore(walRecoveryStatusCoordinatorPending)
	if _, ready := walRecoveryCoordinator(state, 3); ready {
		t.Fatal("stores without recovery status must not elect a coordinator")
	}
	state.LogState.Stores["store-b"] = pb.LogStoreInfo{ConfigData: &pb.ConfigData{
		Content: map[string]*pb.ConfigItem{"other": {CurrentValue: "value"}},
	}}
	if _, ready := walRecoveryCoordinator(state, 3); ready {
		t.Fatal("stores without the recovery status item must not elect a coordinator")
	}
	state.LogState.Stores["store-b"] = recoveryStore("invalid")
	if _, ready := walRecoveryCoordinator(state, 3); ready {
		t.Fatal("stores with an invalid recovery status must not elect a coordinator")
	}
}

func TestValidateWALRecoverySequence(t *testing.T) {
	ctx := context.Background()
	normal := func(dsn uint64, count uint32, raftIndex uint64) WALEntry {
		return WALEntry{
			DSN:        dsn,
			SafeDSN:    dsn - 1,
			RaftIndex:  raftIndex,
			EntryCount: count,
			RawData:    testRawLogEntry(dsn, dsn-1, count),
		}
	}
	skip := func(raftIndex uint64, dsns, psns []uint64) WALEntry {
		return WALEntry{
			RaftIndex:  raftIndex,
			EntryCount: 1,
			RawData:    testRawSkipLogEntryWithPairs(dsns, psns),
		}
	}

	tests := []struct {
		name    string
		entries []WALEntry
		want    string
	}{
		{
			name: "overlapping normal ranges",
			entries: []WALEntry{
				normal(10, 2, 10),
				normal(11, 2, 11),
			},
			want: "overlaps active DSN range",
		},
		{
			name: "skip references missing DSN",
			entries: []WALEntry{
				normal(10, 1, 10),
				skip(11, []uint64{11}, []uint64{10}),
			},
			want: "references unknown DSN",
		},
		{
			name: "skip references wrong PSN",
			entries: []WALEntry{
				normal(10, 1, 10),
				skip(11, []uint64{10}, []uint64{99}),
			},
			want: "PSN mismatch",
		},
		{
			name: "skip repeats DSN",
			entries: []WALEntry{
				normal(10, 1, 10),
				skip(11, []uint64{10, 10}, []uint64{10, 10}),
			},
			want: "repeats DSN",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateWALRecoveryEntries(ctx, tc.entries)
			require.ErrorContains(t, err, tc.want)
		})
	}

	// A skip removes the old mapping and permits that DSN range to be reused.
	require.NoError(t, validateWALRecoveryEntries(ctx, []WALEntry{
		normal(10, 2, 10),
		skip(11, []uint64{10}, []uint64{10}),
		normal(10, 2, 12),
	}))
}

func writeTestWALDataFile(path string, entries []WALEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(entries)))
	if _, err := f.Write(countBuf); err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.RawData == nil && entry.EntryCount > 0 {
			entry.RawData = testRawLogEntry(entry.DSN, entry.SafeDSN, entry.EntryCount)
		}
		header := make([]byte, walDataFileEntryHeaderSize)
		binary.LittleEndian.PutUint64(header[0:8], entry.DSN)
		binary.LittleEndian.PutUint64(header[8:16], entry.SafeDSN)
		binary.LittleEndian.PutUint64(header[16:24], entry.RaftIndex)
		binary.LittleEndian.PutUint64(header[24:32], entry.RaftTerm)
		binary.LittleEndian.PutUint32(header[32:36], entry.EntryCount)
		binary.LittleEndian.PutUint32(header[36:40], uint32(len(entry.RawData)))
		if _, err := f.Write(header); err != nil {
			return err
		}
		if _, err := f.Write(entry.RawData); err != nil {
			return err
		}
	}

	return nil
}

func testRawLogEntry(startDSN, safeDSN uint64, count uint32) []byte {
	const (
		infoSize    = tnEntryInfoHeaderSize
		payloadSize = 1
		tnEntrySize = tnEntryMinSize + infoSize + payloadSize
	)
	footerOffset := logEntryHeaderSize + int(count)*tnEntrySize
	data := make([]byte, footerOffset+int(count)*logEntryFooterRecordSize)
	binary.LittleEndian.PutUint16(data[logEntryTypeOffset:], logEntryTypeWALRecord)
	binary.LittleEndian.PutUint16(data[logEntryVersionOffset:], logEntryVersionV2)
	binary.LittleEndian.PutUint16(data[logEntryCommandOffset:], logEntryCommandNormal)
	binary.LittleEndian.PutUint32(data[logEntryCountOffset:], count)
	binary.LittleEndian.PutUint64(data[logEntryStartDSNOffset:], startDSN)
	binary.LittleEndian.PutUint64(data[logEntrySafeDSNOffset:], safeDSN)
	binary.LittleEndian.PutUint32(data[logEntryFooterOffset:], uint32(footerOffset))
	for i := uint32(0); i < count; i++ {
		offset := logEntryHeaderSize + int(i)*tnEntrySize
		entryData := data[offset : offset+tnEntrySize]
		binary.LittleEndian.PutUint64(entryData, startDSN+uint64(i))
		binary.LittleEndian.PutUint16(entryData[tnEntryTypeOffset:], tnEntryMaxType-2)
		binary.LittleEndian.PutUint16(entryData[tnEntryVersionOffset:], tnEntryVersionV1)
		binary.LittleEndian.PutUint32(entryData[tnEntryPayloadSizeOffset:], payloadSize)
		binary.LittleEndian.PutUint32(entryData[tnEntryInfoSizeOffset:], infoSize)
		entryData[len(entryData)-1] = byte(i + 1)
		pos := footerOffset + int(i)*logEntryFooterRecordSize
		binary.LittleEndian.PutUint32(data[pos:], uint32(offset))
		binary.LittleEndian.PutUint32(data[pos+4:], tnEntrySize)
	}
	return data
}

func testRawSkipLogEntry() []byte {
	return testRawSkipLogEntryWithPairs([]uint64{1}, []uint64{1})
}

func testRawSkipLogEntryWithPairs(dsns, psns []uint64) []byte {
	if len(dsns) != len(psns) {
		panic("skip DSN/PSN length mismatch")
	}
	bodySize := 16 * len(dsns)
	footerOffset := logEntryHeaderSize + bodySize
	data := make([]byte, footerOffset+logEntryFooterRecordSize)
	binary.LittleEndian.PutUint16(data[logEntryTypeOffset:], logEntryTypeWALRecord)
	binary.LittleEndian.PutUint16(data[logEntryVersionOffset:], logEntryVersionV2)
	binary.LittleEndian.PutUint16(data[logEntryCommandOffset:], logEntryCommandSkipDSN)
	binary.LittleEndian.PutUint32(data[logEntryCountOffset:], 1)
	binary.LittleEndian.PutUint32(data[logEntryFooterOffset:], uint32(footerOffset))
	for i, dsn := range dsns {
		binary.LittleEndian.PutUint64(data[logEntryHeaderSize+i*8:], dsn)
	}
	psnOffset := logEntryHeaderSize + len(dsns)*8
	for i, psn := range psns {
		binary.LittleEndian.PutUint64(data[psnOffset+i*8:], psn)
	}
	binary.LittleEndian.PutUint32(data[footerOffset:], logEntryHeaderSize)
	binary.LittleEndian.PutUint32(data[footerOffset+4:], uint32(bodySize))
	return data
}
