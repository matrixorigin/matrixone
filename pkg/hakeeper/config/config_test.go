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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeoutConfig(t *testing.T) {
	c := DefaultTimeoutConfig()
	assert.Equal(t, DefaultTickPerSecond, c.TickPerSecond)
	assert.Equal(t, DefaultLogStoreTimeout, c.LogStoreTimeout)
	assert.Equal(t, DefaultDnStoreTimeout, c.DnStoreTimeout)

	nc := DefaultTimeoutConfig().SetLogStoreTimeout(time.Minute)
	assert.Equal(t, time.Minute, nc.LogStoreTimeout)

	nc = nc.SetTickPerSecond(100)
	assert.Equal(t, 100, nc.TickPerSecond)

	nc = nc.SetDnStoreTimeout(100 * time.Second)
	assert.Equal(t, 100*time.Second, nc.DnStoreTimeout)
}
