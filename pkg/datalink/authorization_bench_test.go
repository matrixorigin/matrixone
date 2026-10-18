// Copyright 2026 Matrix Origin
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
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// BenchmarkDatalinkAuthorizedRead measures the real reader path with a shared
// borrowed service. MemoryFS keeps disk/network noise out of the comparison;
// the parallel case uses eight readers per CPU to expose shared contention.
func BenchmarkDatalinkAuthorizedRead(b *testing.B) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("bench", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { fs.Close(ctx) })
	payload := bytes.Repeat([]byte{1}, 4096)
	if err := fs.Write(ctx, fileservice.IOVector{FilePath: "content", Entries: []fileservice.IOEntry{{Data: payload, Size: int64(len(payload))}}}); err != nil {
		b.Fatal(err)
	}
	proc := &process.Process{Base: &process.BaseProcess{FileService: fs, SessionInfo: process.SessionInfo{Account: "sys", Role: "moadmin"}}}
	proc.ReplaceTopCtx(ctx)
	proc.BuildPipelineContext(ctx)
	dl, err := NewDatalink("file://bench:/content", proc)
	if err != nil {
		b.Fatal(err)
	}
	read := func() error {
		r, err := dl.NewReadCloser(proc)
		if err != nil {
			return err
		}
		n, readErr := io.Copy(io.Discard, r)
		closeErr := r.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n != int64(len(payload)) {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	b.Run("serial", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			if err := read(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.SetParallelism(8)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := read(); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
