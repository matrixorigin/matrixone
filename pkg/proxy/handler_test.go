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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
)

type testProxyHandler struct {
	ctx     context.Context
	st      *stopper.Stopper
	logger  *log.MOLogger
	hc      *mockHAKeeperClient
	mc      clusterservice.MOCluster
	re      *rebalancer
	ru      Router
	closeFn func()
}

func newTestProxyHandler(t *testing.T) *testProxyHandler {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	ctx, cancel := context.WithCancel(context.TODO())
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	re := testRebalancer(t, st, logger, mc)
	return &testProxyHandler{
		ctx:    ctx,
		st:     st,
		logger: logger,
		hc:     hc,
		mc:     mc,
		re:     re,
		ru:     newRouter(mc, re, true),
		closeFn: func() {
			mc.Close()
			st.Stop()
			cancel()
		},
	}
}
