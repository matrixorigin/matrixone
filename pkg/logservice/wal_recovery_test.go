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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestReadWALDataFileOrdersByRaftIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal_data.bin")
	input := []WALEntry{
		{DSN: 1, SafeDSN: 10, RaftIndex: 300, RaftTerm: 1, EntryCount: 1, RawData: []byte{1}},
		{DSN: 2, SafeDSN: 20, RaftIndex: 100, RaftTerm: 1, EntryCount: 1, RawData: []byte{2}},
		{DSN: 3, SafeDSN: 30, RaftIndex: 200, RaftTerm: 1, EntryCount: 1, RawData: []byte{3}},
	}
	if err := writeTestWALDataFile(path, input); err != nil {
		t.Fatal(err)
	}

	s := &Service{runtime: runtime.DefaultRuntime()}
	data, err := s.readWALDataFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
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

func TestReadWALDataFileNormalizesSafeDSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal_data.bin")
	input := []WALEntry{
		{DSN: 10, SafeDSN: 60, RaftIndex: 100, RaftTerm: 1, EntryCount: 11, RawData: testRawLogEntry(60)},
		{DSN: 50, SafeDSN: 60, RaftIndex: 200, RaftTerm: 1, EntryCount: 11, RawData: testRawLogEntry(60)},
		{DSN: 21, SafeDSN: 60, RaftIndex: 300, RaftTerm: 1, EntryCount: 29, RawData: testRawLogEntry(60)},
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
	for _, entry := range data.Entries {
		gotSafeDSNs = append(gotSafeDSNs, entry.SafeDSN)
		gotPayloadSafeDSNs = append(
			gotPayloadSafeDSNs,
			binary.LittleEndian.Uint64(entry.RawData[logEntrySafeDSNOffset:]),
		)
	}

	if want := []uint64{20, 20, 60}; !reflect.DeepEqual(gotSafeDSNs, want) {
		t.Fatalf("unexpected safe DSNs: got %v, want %v", gotSafeDSNs, want)
	}
	if want := []uint64{20, 20, 60}; !reflect.DeepEqual(gotPayloadSafeDSNs, want) {
		t.Fatalf("unexpected payload safe DSNs: got %v, want %v", gotPayloadSafeDSNs, want)
	}
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
			name: "count exceeds file size",
			writeFile: func(path string) error {
				data := make([]byte, walDataFileCountSize)
				binary.LittleEndian.PutUint32(data, 1)
				return os.WriteFile(path, data, 0644)
			},
			wantErrMsg: "exceeds max possible entries",
		},
		{
			name: "data length exceeds limit",
			writeFile: func(path string) error {
				data := make([]byte, walDataFileCountSize+walDataFileEntryHeaderSize)
				binary.LittleEndian.PutUint32(data[:walDataFileCountSize], 1)
				binary.LittleEndian.PutUint32(data[walDataFileCountSize+36:], uint32(maxWALRecoveryEntryDataSize+1))
				return os.WriteFile(path, data, 0644)
			},
			wantErrMsg: "exceeds limit",
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

func testRawLogEntry(safeDSN uint64) []byte {
	data := make([]byte, logEntrySafeDSNOffset+8)
	binary.LittleEndian.PutUint64(data[logEntrySafeDSNOffset:], safeDSN)
	return data
}
