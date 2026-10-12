// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"testing"

	dragonboatsm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/stretchr/testify/require"
)

// Measure the real StateQuery using configuration exported by the basic test
// topology. Constructing configuration needs no running services or SQL setup.
func BenchmarkCheckerStateCopy(b *testing.B) {
	for _, name := range []string{"empty", "basic-two-cn"} {
		b.Run(name, func(b *testing.B) {
			state := hakeeper.NewStateMachine(hakeeper.DefaultHAKeeperShardID, 1)
			items := 0
			if name == "basic-two-cn" {
				dataPath := b.TempDir()
				cluster, err := NewCluster(WithCNCount(2), WithTesting(), func(c *cluster) {
					c.options.dataPath = dataPath
				})
				if cluster != nil {
					b.Cleanup(func() { require.NoError(b, cluster.Close()) })
				}
				require.NoError(b, err)
				index := uint64(1)
				cluster.ForeachServices(func(service ServiceOperator) bool {
					config := service.GetServiceConfig()
					common, err := dumpCommonConfig(config)
					require.NoError(b, err)
					var specific map[string]*pb.ConfigItem
					var heartbeat interface{ Marshal() ([]byte, error) }
					var command func([]byte) []byte
					data := &pb.ConfigData{Content: common}
					switch service.ServiceType() {
					case metadata.ServiceType_CN:
						defaults := cnservice.Config{}
						defaults.SetDefaultValue()
						specific, err = util.DumpConfig(config.getCNServiceConfig(), defaults)
						heartbeat = &pb.CNStoreHeartbeat{UUID: service.ServiceID(), ConfigData: data}
						command = hakeeper.GetCNStoreHeartbeatCmd
					case metadata.ServiceType_TN:
						defaults := tnservice.Config{}
						defaults.SetDefaultValue()
						specific, err = util.DumpConfig(config.getTNServiceConfig(), defaults)
						heartbeat = &pb.TNStoreHeartbeat{UUID: service.ServiceID(), ConfigData: data}
						command = hakeeper.GetTNStoreHeartbeatCmd
					case metadata.ServiceType_LOG:
						specific, err = util.DumpConfig(config.getLogServiceConfig(), logservice.Config{})
						heartbeat = &pb.LogStoreHeartbeat{UUID: service.ServiceID(), ConfigData: data}
						command = hakeeper.GetLogStoreHeartbeatCmd
					default:
						b.Fatalf("unexpected basic fixture service: %s", service.ServiceType())
					}
					require.NoError(b, err)
					for key, item := range specific {
						data.Content[key] = item
					}
					items += len(data.Content)
					encoded, err := heartbeat.Marshal()
					require.NoError(b, err)
					_, err = state.Update(dragonboatsm.Entry{Index: index, Cmd: command(encoded)})
					require.NoError(b, err)
					index++
					return true
				})
			}
			for _, scheduling := range []bool{false, true} {
				label := "full"
				if scheduling {
					label = "scheduling"
				}
				b.Run(label, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := state.Lookup(&hakeeper.StateQuery{Scheduling: scheduling}); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(items), "config-items")
				})
			}
		})
	}
}
