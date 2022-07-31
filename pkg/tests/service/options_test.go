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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
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
	opt := Options{}.WithDnShardNum(num)
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

func TestWithDnTxnStorage(t *testing.T) {
	s := "MEM"
	opt := Options{}.WithDnTxnStorage(s)
	require.Equal(t, s, opt.dn.txnStorageBackend)
}

func TestWithHostAddress(t *testing.T) {
	host := "127.0.0.1"
	opt := Options{}.WithHostAddress(host)
	require.Equal(t, host, opt.hostAddr)
}

func TestWithLogLevel(t *testing.T) {
	lvl := zapcore.WarnLevel
	opt := Options{}.WithLogLvel(lvl)
	require.Equal(t, lvl, opt.logLevel)
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
