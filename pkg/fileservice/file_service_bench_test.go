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
	"testing"

	"github.com/stretchr/testify/assert"
)

func benchmarkFileService(ctx context.Context, b *testing.B, newFS func() FileService) {

	b.Run("parallel raed", func(b *testing.B) {
		fs := newFS()
		defer fs.Close()

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

			for pb.Next() {
				for i := range readVector.Entries {
					readVector.Entries[i].done = false
					readVector.Entries[i].Data = nil
				}
				err := fs.Read(ctx, readVector)
				assert.Nil(b, err)
			}

		})

	})

}
