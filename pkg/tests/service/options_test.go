// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptons(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	require.Equal(t, opt.rootDataDir, defaultRootDataDir)
}

func TestWithTNServiceNum(t *testing.T) {
	num := 4
	opt := Options{}.WithTNServiceNum(num)
	require.Equal(t, num, opt.initial.tnServiceNum)
}

func TestWithLogServiceNum(t *testing.T) {
	num := 5
	opt := Options{}.WithLogServiceNum(num)
	require.Equal(t, num, opt.initial.logServiceNum)
}

func TestWithLogShardNum(t *testing.T) {
	num := uint64(4)
	opt := Options{}.WithLogShardNum(num)
	require.Equal(t, num, opt.initial.logShardNum)
}

func TestWithTnShartnum(t *testing.T) {
	num := uint64(5)
	opt := Options{}.WithTNShardNum(num)
	require.Equal(t, num, opt.initial.tnShardNum)
}

func TestWithLogReplicaNum(t *testing.T) {
	num := uint64(4)
	opt := Options{}.WithLogReplicaNum(num)
	require.Equal(t, num, opt.initial.logReplicaNum)
}

func TestWithRootDataDir(t *testing.T) {
	root := "/tmp/tests"
	opt := Options{}.WithRootDataDir(root)
	require.Equal(t, root, opt.rootDataDir)
}

func TestWithStorage(t *testing.T) {
	opt := Options{}.WithTNUseMEMStorage()
	require.Equal(t, tnservice.StorageMEM, opt.storage.tnStorage)
}

func TestWithHostAddress(t *testing.T) {
	host := "127.0.0.1"
	opt := Options{}.WithHostAddress(host)
	opt.validate()
	require.Equal(t, host, opt.hostAddr)
}

func TestWithHKTickPerSecond(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	require.Equal(t, defaultTickPerSecond, opt.hakeeper.tickPerSecond)

	tick := 11
	opt = opt.WithHKTickPerSecond(tick)
	require.Equal(t, tick, opt.hakeeper.tickPerSecond)
}

func TestWithHKLogStoreTimeout(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	require.Equal(t, defaultLogStoreTimeout, opt.hakeeper.logStoreTimeout)

	timeout := 20 * time.Second
	opt = opt.WithHKLogStoreTimeout(timeout)
	require.Equal(t, timeout, opt.hakeeper.logStoreTimeout)
}

func TestWithHKTNStoreTimeout(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	require.Equal(t, defaultTNStoreTimeout, opt.hakeeper.tnStoreTimeout)

	timeout := 21 * time.Second
	opt = opt.WithHKTNStoreTimeout(timeout)
	require.Equal(t, timeout, opt.hakeeper.tnStoreTimeout)
}

func TestWithHKCNStoreTimeout(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	require.Equal(t, defaultCNStoreTimeout, opt.hakeeper.cnStoreTimeout)

	timeout := 20 * time.Second
	opt = opt.WithHKCNStoreTimeout(timeout)
	require.Equal(t, timeout, opt.hakeeper.cnStoreTimeout)
}

func TestWithTNHeartbeatInterval(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	require.Equal(t, defaultTNHeartbeatInterval, opt.heartbeat.tn)

	interval := 21 * time.Second
	opt = opt.WithTNHeartbeatInterval(interval)
	require.Equal(t, interval, opt.heartbeat.tn)
}

func TestWithLogHeartbeatInterval(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	require.Equal(t, defaultLogHeartbeatInterval, opt.heartbeat.log)

	interval := 22 * time.Second
	opt = opt.WithLogHeartbeatInterval(interval)
	require.Equal(t, interval, opt.heartbeat.log)
}

func TestWithHKCheckInterval(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	require.Equal(t, defaultCheckInterval, opt.hakeeper.checkInterval)

	interval := 23 * time.Second
	opt = opt.WithHKCheckInterval(interval)
	require.Equal(t, interval, opt.hakeeper.checkInterval)
}

func TestGossipSeedNum(t *testing.T) {
	require.Equal(t, 1, gossipSeedNum(1))
	require.Equal(t, 2, gossipSeedNum(2))
}

func TestHaKeeperNum(t *testing.T) {
	require.Equal(t, 1, haKeeperNum(1))
	require.Equal(t, 2, haKeeperNum(2))
}

func TestBuildHAKeeperConfig(t *testing.T) {
	opt := DefaultOptions()
	opt.validate()
	cfg := opt.BuildHAKeeperConfig()
	require.Equal(t, opt.hakeeper.tickPerSecond, cfg.TickPerSecond)
	require.Equal(t, opt.hakeeper.logStoreTimeout, cfg.LogStoreTimeout)
	require.Equal(t, opt.hakeeper.tnStoreTimeout, cfg.TNStoreTimeout)
}
