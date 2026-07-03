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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestParseDatalink(t *testing.T) {

	type testCase struct {
		name          string
		data          string
		wantMoUrl     string
		wantUrlParams []int64
	}
	tests := []testCase{
		{
			name:          "Test1 - File",
			data:          "file:///a/b/c/1.txt",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int64{0, -1},
		},
		{
			name:          "Test2 - File",
			data:          "file:///a/b/c/1.txt?offset=1&size=2",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int64{1, 2},
		},
		{
			name:          "Test3 - File",
			data:          "file:///a/b/c/1.txt?offset=1",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int64{1, -1},
		},
		{
			name:          "Test4 - File",
			data:          "file:///a/b/c/1.txt?size=2",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int64{0, 2},
		},
		{
			name:          "Test5 - File",
			data:          "hdfs://localhost:8888/a/b/c/1.txt?size=2",
			wantMoUrl:     "hdfs,endpoint=localhost:8888\n:/a/b/c/1.txt",
			wantUrlParams: []int64{0, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2, err := ParseDatalink(tt.data, nil)
			if err != nil {
				t.Errorf("ParseDatalink() error = %v", err)
			}
			if !reflect.DeepEqual(got1, tt.wantMoUrl) {
				t.Errorf("ParseDatalink() = %v, want %v", got1, tt.wantMoUrl)
			}
			if !reflect.DeepEqual(got2, tt.wantUrlParams) {
				t.Errorf("ParseDatalink() = %v, want %v", got2, tt.wantUrlParams)
			}
		})
	}
}

func TestParseDatalinkFailed(t *testing.T) {

	type testCase struct {
		name string
		data string
	}
	tests := []testCase{
		{
			name: "Test1 - File",
			data: "s3://a/b/c/1.txt",
		},
		{
			name: "Test2 - File",
			data: "file:///a/b/c/1.txt?offset=-2&size=2",
		},
		{
			name: "Test3 - File",
			data: "file:///a/b/c/1.txt?offset=b",
		},
		{
			name: "Test4 - File",
			data: "file:///a/b/c/1.txt?size=c",
		},
		{
			name: "Test5 - Empty Stage",
			data: "stage:///a/b/c/1.txt?size=c",
		},
		{
			name: "Test6 - Without scheme",
			data: "/a/b/c/1.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ParseDatalink(tt.data, nil)
			if err == nil {
				t.Errorf("ParseDatalink() should error out. %s", tt.data)
			}
		})
	}
}

// newLocalDatalink builds a Datalink for a local file:// path with an optional
// query (?offset=&size=). Fails the test on parse error.
func newLocalDatalink(t *testing.T, proc *process.Process, path, query string) Datalink {
	t.Helper()
	dl, err := NewDatalink("file://"+path+query, proc)
	require.NoError(t, err)
	return dl
}

// TestReadInto exercises Datalink.ReadInto: exact read, buffer reuse (grow +
// shrink with no stale tail), the read-past-EOF short-read branch (io.ReadFull ->
// ErrUnexpectedEOF / EOF returning only the bytes read), and the size<=0 no-op.
func TestReadInto(t *testing.T) {
	dir := t.TempDir()
	proc := testutil.NewProcess(t)
	f := filepath.Join(dir, "f")
	g := filepath.Join(dir, "g")
	require.NoError(t, os.WriteFile(f, []byte("worldwide"), 0o600)) // 9 bytes
	require.NoError(t, os.WriteFile(g, []byte("hi"), 0o600))        // 2 bytes

	var buf []byte
	var err error

	// exact read -> full content, buffer allocated to size
	buf, err = newLocalDatalink(t, proc, f, "?offset=0&size=9").ReadInto(proc, buf, 9)
	require.NoError(t, err)
	require.Equal(t, "worldwide", string(buf))
	require.GreaterOrEqual(t, cap(buf), 9)
	cap0 := cap(buf)

	// reuse for a SMALLER, different file: must overwrite (no stale "rldwide" tail)
	// and must NOT reallocate (cap unchanged -> the buffer was reused).
	buf, err = newLocalDatalink(t, proc, g, "?offset=0&size=2").ReadInto(proc, buf, 2)
	require.NoError(t, err)
	require.Equal(t, "hi", string(buf))
	require.Equal(t, cap0, cap(buf), "buffer should be reused, not reallocated")

	// size PAST eof: LimitReader yields the 9 available bytes, io.ReadFull returns
	// ErrUnexpectedEOF -> ReadInto returns exactly those bytes, no error, no padding.
	buf, err = newLocalDatalink(t, proc, f, "?offset=0&size=100").ReadInto(proc, buf, 100)
	require.NoError(t, err)
	require.Equal(t, "worldwide", string(buf))

	// offset mid-file, size past eof -> partial from offset
	buf, err = newLocalDatalink(t, proc, f, "?offset=2&size=100").ReadInto(proc, buf, 100)
	require.NoError(t, err)
	require.Equal(t, "rldwide", string(buf))

	// offset AT eof -> 0 bytes read, io.EOF handled -> empty, no error
	buf, err = newLocalDatalink(t, proc, f, "?offset=9&size=100").ReadInto(proc, buf, 100)
	require.NoError(t, err)
	require.Len(t, buf, 0)

	// size<=0 -> no read, empty result (caller treats as null)
	buf, err = newLocalDatalink(t, proc, f, "?offset=0&size=9").ReadInto(proc, buf, 0)
	require.NoError(t, err)
	require.Len(t, buf, 0)
}

// TestGetBytes exercises Datalink.GetBytes for both the sized path (d.Size>0 ->
// pre-allocated io.ReadFull, incl. the read-past-EOF short read) and the unsized
// path (d.Size==-1 -> io.ReadAll of the whole file from offset).
func TestGetBytes(t *testing.T) {
	dir := t.TempDir()
	proc := testutil.NewProcess(t)
	f := filepath.Join(dir, "f")
	require.NoError(t, os.WriteFile(f, []byte("worldwide"), 0o600)) // 9 bytes

	// sized path: exact
	b, err := newLocalDatalink(t, proc, f, "?offset=0&size=9").GetBytes(proc)
	require.NoError(t, err)
	require.Equal(t, "worldwide", string(b))

	// sized path: past EOF -> available bytes, no error (io.ReadAll semantics)
	b, err = newLocalDatalink(t, proc, f, "?offset=0&size=100").GetBytes(proc)
	require.NoError(t, err)
	require.Equal(t, "worldwide", string(b))

	// sized path: offset + size slice
	b, err = newLocalDatalink(t, proc, f, "?offset=2&size=3").GetBytes(proc)
	require.NoError(t, err)
	require.Equal(t, "rld", string(b))

	// unsized path (no ?size): d.Size==-1 -> io.ReadAll whole file
	b, err = newLocalDatalink(t, proc, f, "").GetBytes(proc)
	require.NoError(t, err)
	require.Equal(t, "worldwide", string(b))

	// unsized path with offset -> from offset to EOF ("worldwide"[4:] == "dwide")
	b, err = newLocalDatalink(t, proc, f, "?offset=4").GetBytes(proc)
	require.NoError(t, err)
	require.Equal(t, "dwide", string(b))
}
