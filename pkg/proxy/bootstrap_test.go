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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestBootstrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupProcessLevelRuntime(rt)
	c := mockHAKeeperClient{}
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	cfg := Config{
		RebalanceInterval: toml.Duration{Duration: time.Second},
	}
	cfg.Cluster.RefreshInterval = toml.Duration{Duration: defaultRefreshInterval}
	h, err := newProxyHandler(ctx, rt, cfg, st, nil, &c, true)
	require.NoError(t, err)
	h.bootstrap(ctx)

	u, err := db_holder.GetSQLWriterDBUser()
	require.NoError(t, err)
	require.Equal(t, db_holder.MOLoggerUser, u.UserName)
	f := db_holder.GetSQLWriterDBAddressFunc()
	require.NotNil(t, f)
	addr, err := f(ctx, true)
	require.Error(t, err)
	require.Equal(t, "", addr)
}
