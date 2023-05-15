// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/stretchr/testify/require"
)

func WithHAKeeperClient(c logservice.ClusterHAKeeperClient) Option {
	return func(s *Server) {
		s.testHAKeeperClient = c
	}
}

func TestNewServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	temp := os.TempDir()
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress: "unix://" + listenAddr,
	}
	hc := &mockHAKeeperClient{}
	s, err := NewServer(ctx, cfg, WithRuntime(runtime.DefaultRuntime()),
		WithHAKeeperClient(hc))
	defer func() {
		err := s.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)
	require.NotNil(t, s)
}
