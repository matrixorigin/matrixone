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

package tnservice

import (
	"math"
	"testing"

	btoml "github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	motoml "github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	c := &Config{}
	assert.Error(t, c.Validate())
	c.UUID = "dn1"
	assert.NoError(t, c.Validate())
	assert.Equal(t, c.ListenAddress, defaultListenAddress)
	assert.Equal(t, c.ServiceAddress, defaultServiceAddress)
	assert.Equal(t, StorageTAE, c.Txn.Storage.Backend)
	assert.Equal(t, defaultZombieTimeout, c.Txn.ZombieTimeout.Duration)
	assert.Equal(t, defaultDiscoveryTimeout, c.HAKeeper.DiscoveryTimeout.Duration)
	assert.Equal(t, defaultHeatbeatInterval, c.HAKeeper.HeatbeatInterval.Duration)
	assert.Equal(t, defaultHeatbeatTimeout, c.HAKeeper.HeatbeatTimeout.Duration)
	assert.Equal(t, defaultConnectTimeout, c.LogService.ConnectTimeout.Duration)
	assert.Equal(t, defaultReplayReadSize, c.LogService.ReplayReadSize)
	assert.Equal(t, options.DefaultCheckpointIncrementalInterval, c.Ckp.IncrementalInterval.Duration)
	assert.Equal(t, "true", c.Txn.IncrementalDedup)
}

func TestDefaulValue(t *testing.T) {
	c := Config{}
	c.SetDefaultValue()
	assert.Equal(t, options.DefaultCheckpointIncrementalInterval, c.Ckp.IncrementalInterval.Duration)
	assert.Equal(t, defaultReplayReadSize, c.LogService.ReplayReadSize)
}

func TestLogServiceReplayReadSize(t *testing.T) {
	var c Config
	_, err := btoml.Decode(`
[logservice]
replay-read-size = "256MiB"

[rpc]
max-message-size = "320MiB"
`, &c)
	assert.NoError(t, err)
	assert.Equal(t, motoml.ByteSize(256*mpool.MB), c.LogService.ReplayReadSize)

	c.UUID = "dn1"
	assert.NoError(t, c.Validate())
	assert.Equal(t, motoml.ByteSize(256*mpool.MB), c.LogService.ReplayReadSize)
}

func TestLogServiceReplayReadSizeOverflow(t *testing.T) {
	c := Config{UUID: "dn1"}
	c.LogService.ReplayReadSize = motoml.ByteSize(math.MaxInt) + 1
	assert.Error(t, c.Validate())
}

func TestLogServiceReplayReadSizeExceedsRPCMaxMessageSize(t *testing.T) {
	c := Config{UUID: "dn1"}
	c.LogService.ReplayReadSize = motoml.ByteSize(101 * mpool.MB)
	assert.ErrorContains(t, c.Validate(), "exceeds TN RPC max-message-size")
}
