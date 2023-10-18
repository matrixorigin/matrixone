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

package morpc

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
)

func TestAdjustConfig(t *testing.T) {
	c := Config{}
	c.Adjust()
	assert.Equal(t, defaultMaxConnections, c.MaxConnections)
	assert.Equal(t, defaultSendQueueSize, c.SendQueueSize)
	assert.Equal(t, defaultMaxIdleDuration, c.MaxIdleDuration.Duration)
	assert.Equal(t, toml.ByteSize(defaultBufferSize), c.WriteBufferSize)
	assert.Equal(t, toml.ByteSize(defaultBufferSize), c.ReadBufferSize)
	assert.Equal(t, toml.ByteSize(defaultMaxMessageSize), c.MaxMessageSize)
}
