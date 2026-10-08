// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type transferBenchmarkOperator struct{ client.TxnOperator }

func (transferBenchmarkOperator) SnapshotTS() timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: 20}
}

func BenchmarkWorkspaceTombstoneTransfer(b *testing.B) {
	for _, size := range []int{0, 1024, 16384, 65536} {
		b.Run(fmt.Sprintf("insert-only/%d", size), func(b *testing.B) {
			fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.CacheConfig{}, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { fs.Close(context.Background()) })
			proc := &process.Process{Base: &process.BaseProcess{FileService: fs}}
			bat := batch.NewWithSize(0)
			bat.SetRowCount(1)
			txn := &Transaction{proc: proc, op: transferBenchmarkOperator{}, writes: make([]Entry, size)}
			for i := range txn.writes {
				txn.writes[i] = Entry{typ: INSERT, tableId: 1000, bat: bat}
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn.transfer.lastTransferred = types.BuildTS(10, 0)
				txn.Lock()
				err := txn.transferTombstones(ctx)
				txn.Unlock()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
