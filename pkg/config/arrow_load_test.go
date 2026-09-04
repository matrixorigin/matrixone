// Copyright 2026 Matrix Origin
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

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestArrowLoadRolloutGatesDefaultOffAndDecodeIndependently(t *testing.T) {
	var frontend FrontendParameters
	frontend.SetDefaultValues()
	require.Equal(t, ArrowLoadParameters{}, frontend.ArrowLoad)

	var decoded struct {
		ArrowLoad ArrowLoadParameters `toml:"arrow-load"`
	}
	_, err := toml.Decode(`[arrow-load]
enabled = true
s3-enabled = true
distributed-enabled = false
force-materialize = true
`, &decoded)
	require.NoError(t, err)
	require.Equal(t, ArrowLoadParameters{
		Enabled: true, S3Enabled: true, DistributedEnabled: false, ForceMaterialize: true,
	}, decoded.ArrowLoad)
}
