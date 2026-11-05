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

package frontend

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Each op is four resolutions. Use -benchtime=32x -cpu=4 for the same
// 128 operations in every sample and source mode. Each worker owns its
// compiler, process and mock catalog reader; only the original catalog may
// be shared. Setup, priming and full output/source checks are untimed.
func BenchmarkLegacyFloatResolverWorkers(b *testing.B) {
	for _, name := range []string{"temporal_locale30", "nested_precision8"} {
		for _, shared := range []bool{true, false} {
			mode := "different_source"
			if shared {
				mode = "same_source"
			}
			b.Run(name+"/"+mode, func(b *testing.B) {
				source, expected := floatResolverFixture(b, name)
				before := proto.Clone(source)
				var sources, wants [4]*planpb.TableDef
				var resolves [4]func() (*planpb.TableDef, error)
				for worker := range resolves {
					sources[worker] = source
					wants[worker] = expected
					if !shared {
						sources[worker] = proto.Clone(source).(*planpb.TableDef)
						sources[worker].Version += uint32(worker + 1)
						wants[worker] = proto.Clone(expected).(*planpb.TableDef)
						wants[worker].Version = sources[worker].Version
					}
					resolves[worker] = floatResolver(b, sources[worker])
					actual, err := resolves[worker]()
					require.NoError(b, err)
					require.True(b, proto.Equal(wants[worker], actual))
				}
				type result struct {
					table *planpb.TableDef
					err   error
				}
				var results [4]result
				start, done := make(chan struct{}), make(chan struct{}, 4)
				b.ReportAllocs()
				b.ResetTimer()
				for worker := range resolves {
					go func() {
						defer func() { done <- struct{}{} }()
						<-start
						for range b.N {
							actual, err := resolves[worker]()
							if err == nil && (actual == nil || len(actual.Cols) != len(expected.Cols)) {
								err = fmt.Errorf("lost catalog columns")
							}
							results[worker] = result{actual, err}
							if err != nil {
								return
							}
						}
					}()
				}
				close(start)
				for range resolves {
					<-done
				}
				b.StopTimer()
				b.ReportMetric(float64(4*b.N)/b.Elapsed().Seconds(), "resolves/s")
				for worker, result := range results {
					require.NoError(b, result.err)
					require.True(b, proto.Equal(wants[worker], result.table))
					snapshot := proto.Clone(before).(*planpb.TableDef)
					if !shared {
						snapshot.Version = source.Version + uint32(worker+1)
					}
					require.True(b, proto.Equal(snapshot, sources[worker]))
				}
			})
		}
	}
}
