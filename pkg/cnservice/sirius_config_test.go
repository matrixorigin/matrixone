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

package cnservice

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestValidateSiriusConfigKeys(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		invalid     bool
	}{
		{"current schema", "[cn.sirius]\nenabled=false\nnative-config-path='sirius.yaml'\ngpu-streams=4\nmax-waiting-queries=16\nrequest-timeout='1m'\ncleanup-timeout='5s'", false},
		{"disabled unknown scalar", "[cn.sirius]\nenabled=false\nbackend=''", true},
		{"unknown empty table", "[cn.sirius.unused]", true},
		{"case folded section", "[CN.SIRIUS]\nunused=false", true},
		{"unrelated unknown section", "[other]\nunused=false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg struct {
				CN Config `toml:"cn"`
			}
			metadata, err := toml.Decode(tc.input, &cfg)
			require.NoError(t, err)
			err = ValidateSiriusConfigKeys(metadata)
			if tc.invalid {
				require.ErrorContains(t, err, "unknown Sirius configuration key")
			} else {
				require.NoError(t, err)
			}
			if tc.name == "current schema" {
				require.Equal(t, uint32(4), cfg.CN.Sirius.GPUStreams)
				require.Equal(t, uint32(16), cfg.CN.Sirius.MaxWaitingQueries)
				require.Equal(t, time.Minute, cfg.CN.Sirius.RequestTimeout.Duration)
				require.Equal(t, 5*time.Second, cfg.CN.Sirius.CleanupTimeout.Duration)
			}
		})
	}
}
