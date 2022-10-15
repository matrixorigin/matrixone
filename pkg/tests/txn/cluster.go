// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	defaultTestTimeout = time.Minute

	memKVTxnStorage = "MEMKV"
	memTxnStorage   = "MEM"
)

type cluster struct {
	t       *testing.T
	logger  *zap.Logger
	clock   clock.Clock
	env     service.Cluster
	stopper *stopper.Stopper
}

// NewCluster new txn testing cluster based on the service.Cluster
func NewCluster(t *testing.T, options service.Options) (Cluster, error) {
	env, err := service.NewCluster(t, options)
	if err != nil {
		return nil, err
	}
	stopper := stopper.NewStopper("test-env-stopper")
	return &cluster{
		t:       t,
		env:     env,
		logger:  logutil.GetPanicLoggerWithLevel(zap.DebugLevel),
		clock:   clock.NewUnixNanoHLCClockWithStopper(stopper, 0),
		stopper: stopper,
	}, nil
}

func (c *cluster) Start() {
	if err := c.env.Start(); err != nil {
		assert.FailNow(c.t, "start testing cluster failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	c.env.WaitHAKeeperState(ctx, logservice.HAKeeperRunning)
	c.env.WaitHAKeeperLeader(ctx)
	c.env.WaitDNShardsReported(ctx)
}

func (c *cluster) Stop() {
	if err := c.env.Close(); err != nil {
		assert.FailNow(c.t, "stop testing cluster failed")
	}
}

func (c *cluster) Env() service.Cluster {
	return c.env
}

func (c *cluster) NewClient() Client {
	backend := c.env.Options().GetTxnStorageBackend()
	switch backend {
	case memKVTxnStorage:
		cli, err := newKVClient(c.env, c.clock, c.logger)
		require.NoError(c.t, err)
		return cli
	case memTxnStorage:
		cli, err := newSQLClient(c.env)
		require.NoError(c.t, err)
		return cli
	default:
		panic(fmt.Sprintf("%s backend txn storage not support", backend))
	}
}
