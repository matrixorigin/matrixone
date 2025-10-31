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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/stretchr/testify/assert"
)

func benchmarkFileService(ctx context.Context, b *testing.B, newFS func() FileService) {

	b.Run("parallel raed", func(b *testing.B) {
		fs := newFS()
		defer fs.Close(ctx)

		content := bytes.Repeat([]byte("x"), 16*1024*1024)
		parts := fixedSplit(content, 512*1024)
		writeVector := IOVector{
			FilePath: "foo",
		}
		offset := int64(0)
		for _, part := range parts {
			writeVector.Entries = append(writeVector.Entries, IOEntry{
				Offset:      offset,
				Size:        int64(len(part)),
				Data:        part,
				ToCacheData: CacheOriginalData,
			})
			offset += int64(len(part))
		}
		err := fs.Write(ctx, writeVector)
		assert.Nil(b, err)

		parts2 := fixedSplit(content, 4*1024)

		b.SetBytes(int64(len(content)))
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {

			for pb.Next() {
				readVector := &IOVector{
					FilePath: "foo",
				}
				offset := int64(0)
				for _, part := range parts2 {
					readVector.Entries = append(readVector.Entries, IOEntry{
						Offset:      offset,
						Size:        int64(len(part)),
						ToCacheData: CacheOriginalData,
					})
					offset += int64(len(part))
				}
				err := fs.Read(ctx, readVector)
				assert.Nil(b, err)
				readVector.Release()
			}

		})

	})

	b.Run("write", func(b *testing.B) {
		b.Run("1K", func(b *testing.B) {
			benchmarkWrite(ctx, b, common.KiB, newFS)
		})
		b.Run("4K", func(b *testing.B) {
			benchmarkWrite(ctx, b, 4*common.KiB, newFS)
		})
		b.Run("128K", func(b *testing.B) {
			benchmarkWrite(ctx, b, 128*common.KiB, newFS)
		})
	})

}

func benchmarkWrite(ctx context.Context, b *testing.B, fileSize int64, newFS func() FileService) {
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(fileSize)

	fs := newFS()
	defer fs.Close(ctx)

	var n atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			filePath := fmt.Sprintf("benchmark_write_%d_%d_%d", fileSize, b.N, n.Add(1))

			vector := IOVector{
				FilePath: filePath,
				Entries: []IOEntry{
					{
						Size: fileSize,
						Data: data,
					},
				},
			}

			err := fs.Write(ctx, vector)
			if err != nil {
				b.Fatal(err)
			}

		}
	})
}
