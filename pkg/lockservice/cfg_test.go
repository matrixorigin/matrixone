// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"testing"
	"time"

	tp "github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdjustConfig(t *testing.T) {
	c := Config{ServiceID: "s1"}
	c.Validate()
	assert.NotEmpty(t, c.ListenAddress)
	assert.NotEmpty(t, c.ServiceAddress)
	assert.NotEmpty(t, c.KeepBindDuration)
	assert.NotEmpty(t, c.KeepRemoteLockDuration)
	assert.NotNil(t, c.RemoteLockOwnerWaitTimeout)
	assert.NotEmpty(t, c.RemoteLockOwnerWaitTimeout.Duration)
	assert.NotEmpty(t, c.MaxFixedSliceSize)
}

func TestRemoteLockOwnerWaitTimeoutCanBeDisabled(t *testing.T) {
	var c Config
	_, err := tp.Decode(`remote-lock-owner-wait-timeout = "0s"`, &c)
	require.NoError(t, err)
	c.ServiceID = "s1"
	c.Validate()
	require.NotNil(t, c.RemoteLockOwnerWaitTimeout)
	assert.Equal(t, time.Duration(0), c.RemoteLockOwnerWaitTimeout.Duration)
}
