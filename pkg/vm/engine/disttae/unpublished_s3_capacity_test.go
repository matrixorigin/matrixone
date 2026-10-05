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

package disttae

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

type capacityBenchFileService struct {
	fileservice.FileService
	release <-chan struct{}
}

func (fs *capacityBenchFileService) Delete(ctx context.Context, _ ...string) error {
	select {
	case <-fs.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Measure live debt after GC, including names, admission, owners, the real
// transaction-detached callbacks, queue storage/timestamps and FS references.
// Shared FileService caches/object payloads are not per-ticket cleanup state.
// Run with -benchtime=1x: this measures capacity, not throughput. No S3 data is
// uploaded; the production receive/rollback handoff creates the retained graph.
func BenchmarkUnpublishedS3RetainedCapacity(b *testing.B) {
	for _, nameBytes := range []int{42, 256} {
		for _, perTask := range []int{1, 64} {
			b.Run(fmt.Sprintf("name%d/objects-per-task%d", nameBytes, perTask), func(b *testing.B) {
				proc, _, baseFS := newSpillBenchProcess(b)
				op := &spillBenchTxnOperator{}
				proc.Base.TxnOperator = op
				var retained int64
				for range b.N {
					func() {
						server := colexec.NewServer(proc.GetService())
						release := make(chan struct{})
						defer func() {
							close(release)
							require.NoError(b, server.CloseUnpublishedS3Cleanup(context.Background()))
							require.Zero(b, server.UnpublishedS3AdmissionStats().Used)
						}()
						fs := &capacityBenchFileService{FileService: baseFS, release: release}
						limit := server.UnpublishedS3AdmissionStats().Limit
						runtime.GC()
						var before, after runtime.MemStats
						runtime.ReadMemStats(&before)
						for i := 0; i < limit; i += perTask {
							txn := &Transaction{}
							op.workspace = txn
							names := make([]string, perTask)
							for j := range names {
								names[j] = fmt.Sprintf("%0*d", nameBytes, i+j)
							}
							require.NoError(b, colexec.RetainReceivedUnpublishedS3ObjectNames(proc, fs, names...))
							require.NoError(b, txn.QueueUnpublishedS3Cleanup(server))
						}
						op.workspace = nil
						runtime.GC()
						runtime.ReadMemStats(&after)
						require.Equal(b, limit, server.UnpublishedS3AdmissionStats().Used)
						delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
						require.Positive(b, delta)
						retained += delta
						runtime.KeepAlive(server)
					}()
				}
				b.ReportMetric(float64(retained)/float64(b.N), "retained-B")
				b.ReportMetric(float64(retained)/float64(b.N*65_536), "retained-B/ticket")
			})
		}
	}
}
