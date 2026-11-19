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

package fileservice

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
)

func TestIOEntriesReaderZeroFillsGaps(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []IOEntry
		want    []byte
	}{
		{name: "prefix and internal gaps", entries: []IOEntry{
			{Offset: 2, Size: 2, Data: []byte("AB")},
			{Offset: 6, Size: 1, Data: []byte("Z")},
		}, want: []byte{0, 0, 'A', 'B', 0, 0, 'Z'}},
		{name: "contiguous", entries: []IOEntry{
			{Offset: 0, Size: 2, Data: []byte("AB")},
			{Offset: 2, Size: 1, Data: []byte("Z")},
		}, want: []byte("ABZ")},
	} {
		for _, size := range []int{1, 2, 3, 8} {
			t.Run(fmt.Sprintf("%s/buffer-%d", tc.name, size), func(t *testing.T) {
				reader := newIOEntriesReader(context.Background(), tc.entries)
				var got []byte
				for {
					buf := bytes.Repeat([]byte{0xa5}, size)
					n, err := reader.Read(buf)
					got = append(got, buf[:n]...)
					if !bytes.Equal(buf[n:], bytes.Repeat([]byte{0xa5}, size-n)) {
						t.Fatalf("reader changed bytes beyond returned length %d", n)
					}
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatalf("read: %v", err)
					}
					if n == 0 {
						t.Fatal("reader made no progress")
					}
				}
				if !bytes.Equal(got, tc.want) {
					t.Fatalf("got %x, want %x", got, tc.want)
				}
			})
		}
	}
}

func TestFileServiceZeroFillsWriteGaps(t *testing.T) {
	for _, backend := range []string{"memory", "local"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var fs FileService
			var err error
			if backend == "memory" {
				fs, err = NewMemoryFS("test", DisabledCacheConfig, nil)
			} else {
				fs, err = NewLocalFS2(ctx, "test", t.TempDir(), DisabledCacheConfig, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer fs.Close(ctx)
			write := IOVector{FilePath: "gap", Entries: []IOEntry{
				{Offset: 2, Size: 2, Data: []byte("AB")},
				{Offset: 6, Size: 1, Data: []byte("Z")},
			}}
			if err := fs.Write(ctx, write); err != nil {
				t.Fatal(err)
			}
			read := &IOVector{FilePath: "gap", Entries: []IOEntry{{Offset: 0, Size: 7}}}
			if err := fs.Read(ctx, read); err != nil {
				t.Fatal(err)
			}
			defer read.Release()
			if want := []byte{0, 0, 'A', 'B', 0, 0, 'Z'}; !bytes.Equal(read.Entries[0].Data, want) {
				t.Fatalf("got %x, want %x", read.Entries[0].Data, want)
			}
		})
	}
}

func TestIOEntriesReaderZeroFillsGapBeforeStream(t *testing.T) {
	for _, size := range []int64{2, -1} {
		t.Run(fmt.Sprintf("declared-size-%d", size), func(t *testing.T) {
			reader := newIOEntriesReader(context.Background(), []IOEntry{{
				Offset: 3, Size: size, ReaderForWrite: bytes.NewReader([]byte("AB")),
			}})
			var got []byte
			for {
				buf := []byte{0xa5, 0xa5}
				n, err := reader.Read(buf)
				got = append(got, buf[:n]...)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					t.Fatal("reader made no progress")
				}
			}
			if want := []byte{0, 0, 0, 'A', 'B'}; !bytes.Equal(got, want) {
				t.Fatalf("got %x, want %x", got, want)
			}
		})
	}
}
