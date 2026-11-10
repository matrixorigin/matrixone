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

package options

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestLogtailServerCfg(t *testing.T) {
	validated := &LogtailServerCfg{}
	validated.Validate()
	defaults := NewDefaultLogtailServerCfg()
	require.Equal(t, defaults.RpcMaxMessageSize, validated.RpcMaxMessageSize)
	require.Equal(t, defaults.LogtailCollectInterval, validated.LogtailCollectInterval)
	require.Equal(t, defaults.ResponseSendTimeout, validated.ResponseSendTimeout)
	require.Equal(t, defaults.RPCStreamPoisonTime, validated.RPCStreamPoisonTime)
	require.Equal(t, defaults.PullWorkerPoolSize, validated.PullWorkerPoolSize)
}

func TestOptionsFillDefaultsCheckpointIncrementalInterval(t *testing.T) {
	fs := &fileservice.LocalFS{}
	opts := (&Options{Fs: fs, LocalFs: fs, TmpFs: &fileservice.TmpFileService{}}).FillDefaults("")
	require.Equal(t, DefaultCheckpointIncrementalInterval, opts.CheckpointCfg.IncrementalInterval)
}

func TestOptionsFillDefaultsCatalogGCInterval(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *CatalogCfg
		want time.Duration
	}{
		{name: "omitted", want: 3 * time.Minute},
		{name: "zero", cfg: &CatalogCfg{}, want: 3 * time.Minute},
		{name: "explicit", cfg: &CatalogCfg{GCInterval: time.Second}, want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fileservice.LocalFS{}
			opts := (&Options{Fs: fs, LocalFs: fs, TmpFs: &fileservice.TmpFileService{}, CatalogCfg: tc.cfg}).FillDefaults("")
			require.Equal(t, tc.want, opts.CatalogCfg.GCInterval)
		})
	}
}
