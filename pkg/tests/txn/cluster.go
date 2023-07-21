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
)

type cluster struct {
	t       *testing.T
	logger  *zap.Logger
	clock   clock.Clock
	env     service.Cluster
	stopper *stopper.Stopper
}

// NewCluster new txn testing cluster based on the service.Cluster
func NewCluster(ctx context.Context, t *testing.T, options service.Options) (Cluster, error) {
	logger := logutil.GetPanicLoggerWithLevel(zap.DebugLevel)
	env, err := service.NewCluster(ctx, t, options.WithLogger(logger))
	if err != nil {
		return nil, err
	}
	stopper := stopper.NewStopper("test-env-stopper")
	return &cluster{
		t:       t,
		env:     env,
		logger:  logger,
		clock:   clock.NewUnixNanoHLCClockWithStopper(stopper, 0),
		stopper: stopper,
	}, nil
}

func (c *cluster) GetLogger() *zap.Logger {
	return c.logger
}

func (c *cluster) Start() {
	if err := c.env.Start(); err != nil {
		assert.FailNow(c.t, fmt.Sprintf("start testing cluster failed, %v", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	c.env.WaitHAKeeperState(ctx, logservice.HAKeeperRunning)
	c.env.WaitHAKeeperLeader(ctx)
	c.env.WaitDNShardsReported(ctx)
}

func (c *cluster) Stop() {
	c.logger.Info("cluster start stop")
	if err := c.env.Close(); err != nil {
		assert.FailNow(c.t, "stop testing cluster failed")
	}
	c.logger.Info("cluster stop completed")
}

func (c *cluster) Env() service.Cluster {
	return c.env
}

func (c *cluster) NewClient() Client {
	cli, err := newSQLClient(c.env)
	require.NoError(c.t, err)
	return cli
}
