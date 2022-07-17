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

package dn

import (
	"bytes"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
)

func TestCfg(t *testing.T) {
	c := &Config{}
	c.RPC.BusyQueueSize = 10

	buf := bytes.NewBuffer(nil)
	enc := toml.NewEncoder(buf)
	assert.NoError(t, enc.Encode(c))
	assert.Equal(t, "", buf.String())
}
