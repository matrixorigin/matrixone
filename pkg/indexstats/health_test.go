// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexstats

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnalyzeOptions(t *testing.T) {
	opts, err := ParseAnalyzeOptions(context.Background(), "{}")
	require.NoError(t, err)
	require.Equal(t, 100, opts.SampleCount)
	require.Equal(t, .95, opts.RecallTarget)
	opts, err = ParseAnalyzeOptions(context.Background(), `{"sample_count":1,"recall_target":1,"retrain":true}`)
	require.NoError(t, err)
	require.True(t, opts.Retrain)
	for _, raw := range []string{"", `null`, `[]`, `{`, `{} {}`, `{"unknown":1}`, `{"sample_count":0}`, `{"sample_count":257}`, `{"recall_target":0}`, `{"recall_target":1.1}`} {
		_, err := ParseAnalyzeOptions(context.Background(), raw)
		require.Error(t, err, raw)
	}
}
