// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cnservice

import (
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestAutoIDCacheTestConfigsOptIn(t *testing.T) {
	var defaults Config
	require.False(t, defaults.AutoIncrement.EnableAutoIDCache)
	for _, name := range []string{"launch/cn.toml", "launch-multi-cn/cn1.toml", "launch-multi-cn/cn2.toml", "launch-tae-compose/config/cn-0.toml", "launch-tae-compose/config/cn-1.toml"} {
		t.Run(name, func(t *testing.T) {
			var cfg struct {
				CN Config `toml:"cn"`
			}
			_, err := toml.DecodeFile(filepath.Join("../../etc", name), &cfg)
			require.NoError(t, err)
			require.True(t, cfg.CN.AutoIncrement.EnableAutoIDCache)
		})
	}
}
