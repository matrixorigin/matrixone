// Copyright 2021 Matrix Origin
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

package checkpoint

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCfgFillDefaultsIncrementalInterval(t *testing.T) {
	cfg := new(CheckpointCfg)
	cfg.FillDefaults()
	require.Equal(t, options.DefaultCheckpointIncrementalInterval, cfg.IncrementalInterval)
}

func TestCheckpointIntentOldAgeTracksIncrementalInterval(t *testing.T) {
	interval := 7 * time.Minute
	require.Equal(t, interval, checkpointIntentOldAge(interval))
	require.Equal(t, options.DefaultCheckpointIncrementalInterval, checkpointIntentOldAge(0))
}

func TestCheckpointArenaDrainDelayTracksIncrementalInterval(t *testing.T) {
	interval := 7 * time.Minute
	require.Equal(t, 2*interval, checkpointArenaDrainDelay(interval))
}
