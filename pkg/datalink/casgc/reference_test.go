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

package casgc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// realHash returns the sha256 hex digest of the given string, for use as a
// valid contenthash in tests.
func realHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestParseContentHash(t *testing.T) {
	hash1 := realHash("blob-content-1")
	hash2 := realHash("blob-content-2")

	tests := []struct {
		name      string
		raw       string
		wantHash  string
		wantFound bool
	}{
		{
			name:      "valid URL with contenthash",
			raw:       fmt.Sprintf("mo://bucket/path/file.txt?contenthash=%s", hash1),
			wantHash:  hash1,
			wantFound: true,
		},
		{
			name:      "valid URL without contenthash param",
			raw:       "mo://bucket/path/file.txt?other=value",
			wantHash:  "",
			wantFound: false,
		},
		{
			name:      "valid URL with invalid contenthash (too short)",
			raw:       "mo://bucket/path/file.txt?contenthash=ab12",
			wantHash:  "",
			wantFound: false,
		},
		{
			name:      "valid URL with another valid contenthash",
			raw:       fmt.Sprintf("file:///data/something?contenthash=%s", hash2),
			wantHash:  hash2,
			wantFound: true,
		},
		{
			name:      "empty string",
			raw:       "",
			wantHash:  "",
			wantFound: false,
		},
		{
			name:      "valid URL with non-hex contenthash",
			raw:       "mo://bucket/path/file.txt?contenthash=gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
			wantHash:  "",
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotHash, gotFound := parseContentHash(tc.raw)
			if gotFound != tc.wantFound {
				t.Errorf("parseContentHash(%q) found=%v, want %v", tc.raw, gotFound, tc.wantFound)
			}
			if gotHash != tc.wantHash {
				t.Errorf("parseContentHash(%q) hash=%q, want %q", tc.raw, gotHash, tc.wantHash)
			}
		})
	}
}

func TestIsDatalinkType(t *testing.T) {
	datalinkType := types.New(types.T_datalink, 0, 0)
	datalinkBytes := types.EncodeType(&datalinkType)

	varcharType := types.New(types.T_varchar, 255, 0)
	varcharBytes := types.EncodeType(&varcharType)

	tests := []struct {
		name   string
		atttyp []byte
		want   bool
	}{
		{
			name:   "datalink type",
			atttyp: datalinkBytes,
			want:   true,
		},
		{
			name:   "varchar type",
			atttyp: varcharBytes,
			want:   false,
		},
		{
			name:   "empty bytes",
			atttyp: []byte{},
			want:   false,
		},
		{
			name:   "nil bytes",
			atttyp: nil,
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isDatalinkType(tc.atttyp)
			if got != tc.want {
				t.Errorf("isDatalinkType(%v) = %v, want %v", tc.atttyp, got, tc.want)
			}
		})
	}
}

// fakeScanner is a test implementation of rowScanner.
type fakeScanner struct {
	rows map[columnRef][]string
	err  map[columnRef]error
}

func (f *fakeScanner) scanColumn(_ context.Context, ref columnRef, _ string) ([]string, error) {
	if err, ok := f.err[ref]; ok {
		return nil, err
	}
	return f.rows[ref], nil
}

func TestCollectHashesFromColumns(t *testing.T) {
	hash1 := realHash("first-blob")
	hash2 := realHash("second-blob")

	col1 := columnRef{DBName: "mydb", TableName: "mytable", ColName: "dl_col"}

	t.Run("two valid hashes, one missing param, one empty string", func(t *testing.T) {
		sc := &fakeScanner{
			rows: map[columnRef][]string{
				col1: {
					fmt.Sprintf("mo://bucket/a?contenthash=%s", hash1),
					fmt.Sprintf("mo://bucket/b?contenthash=%s", hash2),
					"mo://bucket/c?other=value", // no contenthash
					"",                          // empty string
				},
			},
		}

		got, err := collectHashesFromColumns(context.Background(), sc, []columnRef{col1}, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("expected 2 hashes, got %d: %v", len(got), got)
		}
		if _, ok := got[hash1]; !ok {
			t.Errorf("expected hash1 %q in result", hash1)
		}
		if _, ok := got[hash2]; !ok {
			t.Errorf("expected hash2 %q in result", hash2)
		}
	})

	t.Run("empty columns list returns empty non-nil map", func(t *testing.T) {
		sc := &fakeScanner{rows: map[columnRef][]string{}}
		got, err := collectHashesFromColumns(context.Background(), sc, nil, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected non-nil map")
		}
		if len(got) != 0 {
			t.Fatalf("expected empty map, got %v", got)
		}
	})

	t.Run("scan error propagates", func(t *testing.T) {
		scanErr := errors.New("scan failed")
		sc := &fakeScanner{
			err: map[columnRef]error{col1: scanErr},
		}
		_, err := collectHashesFromColumns(context.Background(), sc, []columnRef{col1}, "")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, scanErr) {
			t.Errorf("expected scanErr, got %v", err)
		}
	})
}
