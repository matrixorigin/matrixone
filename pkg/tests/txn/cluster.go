// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" 	,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/stretchr/testify/assert"
)

var (
	defaultTestTimeout = time.Minute
)

type cluster struct {
	t   *testing.T
	env service.Cluster
}

// NewCluster new txn testing cluster based on the service.Cluster
func NewCluster(t *testing.T, options service.Options) (Cluster, error) {
	env, err := service.NewCluster(t, options)
	if err != nil {
		return nil, err
	}
	return &cluster{
		t:   t,
		env: env,
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

func (c *cluster) Restart() {
	c.Stop()
	c.Start()
}

func (c *cluster) Env() service.Cluster {
	return c.env
}

func (c *cluster) NewClient() Client {
	return nil
}
