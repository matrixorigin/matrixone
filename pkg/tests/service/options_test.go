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

	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptons(t *testing.T) {
	opt := DefaultOptions()
	require.Equal(t, opt.rootDataDir, defaultRootDataDir)
}

func TestWithDNServiceNum(t *testing.T) {
	num := 4
	opt := Options{}.WithDNServiceNum(num)
	require.Equal(t, num, opt.initial.dnServiceNum)
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

func TestWithDnShardNum(t *testing.T) {
	num := uint64(5)
	opt := Options{}.WithDNShardNum(num)
	require.Equal(t, num, opt.initial.dnShardNum)
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
	opt := Options{}.WithDNUseMEMStorage()
	require.Equal(t, dnservice.StorageMEM, opt.storage.dnStorage)
}

func TestWithHostAddress(t *testing.T) {
	host := "127.0.0.1"
	opt := Options{}.WithHostAddress(host)
	require.Equal(t, host, opt.hostAddr)
}

func TestWithHKTickPerSecond(t *testing.T) {
	opt := DefaultOptions()
	require.Equal(t, defaultTickPerSecond, opt.hakeeper.tickPerSecond)

	tick := 11
	opt = opt.WithHKTickPerSecond(tick)
	require.Equal(t, tick, opt.hakeeper.tickPerSecond)
}

func TestWithHKLogStoreTimeout(t *testing.T) {
	opt := DefaultOptions()
	require.Equal(t, defaultLogStoreTimeout, opt.hakeeper.logStoreTimeout)

	timeout := 20 * time.Second
	opt = opt.WithHKLogStoreTimeout(timeout)
	require.Equal(t, timeout, opt.hakeeper.logStoreTimeout)
}

func TestWithHKDNStoreTimeout(t *testing.T) {
	opt := DefaultOptions()
	require.Equal(t, defaultDNStoreTimeout, opt.hakeeper.dnStoreTimeout)

	timeout := 21 * time.Second
	opt = opt.WithHKDNStoreTimeout(timeout)
	require.Equal(t, timeout, opt.hakeeper.dnStoreTimeout)
}

func TestWithHKCNStoreTimeout(t *testing.T) {
	opt := DefaultOptions()
	require.Equal(t, defaultCNStoreTimeout, opt.hakeeper.cnStoreTimeout)

	timeout := 20 * time.Second
	opt = opt.WithHKCNStoreTimeout(timeout)
	require.Equal(t, timeout, opt.hakeeper.cnStoreTimeout)
}

func TestWithDNHeartbeatInterval(t *testing.T) {
	opt := DefaultOptions()
	require.Equal(t, defaultDNHeartbeatInterval, opt.heartbeat.dn)

	interval := 21 * time.Second
	opt = opt.WithDNHeartbeatInterval(interval)
	require.Equal(t, interval, opt.heartbeat.dn)
}

func TestWithLogHeartbeatInterval(t *testing.T) {
	opt := DefaultOptions()
	require.Equal(t, defaultLogHeartbeatInterval, opt.heartbeat.log)

	interval := 22 * time.Second
	opt = opt.WithLogHeartbeatInterval(interval)
	require.Equal(t, interval, opt.heartbeat.log)
}

func TestWithHKCheckInterval(t *testing.T) {
	opt := DefaultOptions()
	require.Equal(t, defaultCheckInterval, opt.hakeeper.checkInterval)

	interval := 23 * time.Second
	opt = opt.WithHKCheckInterval(interval)
	require.Equal(t, interval, opt.hakeeper.checkInterval)
}

func TestGossipSeedNum(t *testing.T) {
	require.Equal(t, 1, gossipSeedNum(1))
	require.Equal(t, 2, gossipSeedNum(2))
	require.Equal(t, 3, gossipSeedNum(3))
	require.Equal(t, 3, gossipSeedNum(4))
}

func TestHaKeeperNum(t *testing.T) {
	require.Equal(t, 1, haKeeperNum(1))
	require.Equal(t, 1, haKeeperNum(2))
	require.Equal(t, 3, haKeeperNum(3))
	require.Equal(t, 3, haKeeperNum(4))
}

func TestBuildHAKeeperConfig(t *testing.T) {
	opt := DefaultOptions()
	cfg := opt.BuildHAKeeperConfig()
	require.Equal(t, opt.hakeeper.tickPerSecond, cfg.TickPerSecond)
	require.Equal(t, opt.hakeeper.logStoreTimeout, cfg.LogStoreTimeout)
	require.Equal(t, opt.hakeeper.dnStoreTimeout, cfg.DNStoreTimeout)
}
