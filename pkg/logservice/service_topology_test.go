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

package logservice

import (
	"errors"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
)

// newTestServiceTopologyWithRetry treats one topology generation as a single
// attempt. A bind collision closes every service already created in that
// generation before all configs and their inter-node addresses are rebuilt.
func newTestServiceTopologyWithRetry(
	genConfigs func() ([]Config, error),
	opts ...Option,
) ([]Config, []*Service, error) {
	var lastErr error
	for attempt := 1; attempt <= serviceStartMaxAttempts; attempt++ {
		configs, err := genConfigs()
		if err != nil {
			return nil, nil, err
		}

		services := make([]*Service, 0, len(configs))
		for _, cfg := range configs {
			service, err := NewService(cfg, newFS(), nil, opts...)
			if err == nil {
				services = append(services, service)
				continue
			}

			if closeErr := closeTestServiceTopology(services); closeErr != nil {
				return nil, nil, errutil.Wrapf(
					errors.Join(err, closeErr),
					"failed to clean up test topology generation",
				)
			}
			if !isAddressAlreadyInUseError(err) {
				return nil, nil, err
			}
			lastErr = err
			break
		}
		if len(services) == len(configs) {
			return configs, services, nil
		}
	}
	return nil, nil, errutil.Wrapf(
		lastErr,
		"failed to create log service topology after %d attempts",
		serviceStartMaxAttempts,
	)
}

func closeTestServiceTopology(services []*Service) error {
	var err error
	for i := len(services) - 1; i >= 0; i-- {
		if services[i] != nil {
			err = errors.Join(err, services[i].Close())
		}
	}
	return err
}

func TestServiceTopologyRetryRebuildsWholeGenerationAfterBindRace(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()

			var (
				attempts         int
				blocker          net.Listener
				failedGeneration []Config
				allConfigs       []Config
			)
			defer func() {
				for i := range allConfigs {
					vfs.ReportLeakedFD(allConfigs[i].FS, t)
				}
			}()

			genConfigs := func() ([]Config, error) {
				attempts++
				configs := make([]Config, 2)
				for i := range configs {
					configs[i] = DefaultConfig()
					configs[i].UUID = uuid.New().String()
					configs[i].FS = vfs.NewStrictMem()
					configs[i].DeploymentID = 1
					configs[i].DisableWorkers = true
				}
				if err := allocateTestConfigPorts(&configs[0], &configs[1]); err != nil {
					return nil, err
				}
				if attempts == 1 {
					var err error
					blocker, err = net.Listen("tcp4", "127.0.0.1:0")
					if err != nil {
						return nil, err
					}
					t.Cleanup(func() {
						if blocker != nil {
							_ = blocker.Close()
						}
					})
					configs[1].LogServicePort = 0
					configs[1].ServiceAddress = blocker.Addr().String()
					configs[1].ServiceListenAddress = blocker.Addr().String()
				}
				configs[0].GossipSeedAddresses = []string{configs[1].GossipServiceAddr()}
				configs[1].GossipSeedAddresses = []string{configs[0].GossipServiceAddr()}
				setTestHAKeeperClientConfig(&configs[0])
				setTestHAKeeperClientConfig(&configs[1])
				for i := range configs {
					runtime.SetupServiceBasedRuntime(configs[i].UUID, rt)
				}
				allConfigs = append(allConfigs, configs...)

				if attempts == 1 {
					failedGeneration = append(failedGeneration, configs...)
				}
				return configs, nil
			}

			configs, services, err := newTestServiceTopologyWithRetry(genConfigs)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, closeTestServiceTopology(services))
			}()
			require.NoError(t, blocker.Close())
			blocker = nil

			require.Equal(t, 2, attempts)
			require.Len(t, failedGeneration, 2)
			require.Len(t, configs, 2)
			for i := range configs {
				require.NotEqual(
					t,
					failedGeneration[i].LogServiceListenAddr(),
					configs[i].LogServiceListenAddr(),
				)
				require.NotEqual(
					t,
					failedGeneration[i].RaftListenAddr(),
					configs[i].RaftListenAddr(),
				)
				require.NotEqual(
					t,
					failedGeneration[i].GossipListenAddr(),
					configs[i].GossipListenAddr(),
				)
			}

			listener, err := net.Listen("tcp4", failedGeneration[0].LogServiceListenAddr())
			require.NoError(t, err, "the partially started service was not closed")
			require.NoError(t, listener.Close())
		},
	)
}
