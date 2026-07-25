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

package jobs

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestValidateMergeEntrySourceLineageCompatibility(t *testing.T) {
	newStats := func(opts ...objectio.ObjectStatsOptions) *objectio.ObjectStats {
		id := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&id, false, true, false)
		for _, opt := range opts {
			opt(stats)
		}
		return stats
	}

	tests := []struct {
		name    string
		version uint32
		stats   *objectio.ObjectStats
		reject  bool
	}{
		{
			name:  "legacy producer and pure TN source",
			stats: newStats(),
		},
		{
			name:   "legacy producer and direct CN source",
			stats:  newStats(objectio.WithCNCreated()),
			reject: true,
		},
		{
			name:   "legacy producer and rewritten CN source",
			stats:  newStats(objectio.WithCNOrigin()),
			reject: true,
		},
		{
			name:    "current producer and direct CN source",
			version: api.MergeCommitEntryLineageVersion,
			stats:   newStats(objectio.WithCNCreated()),
		},
		{
			name:    "future producer retains the current contract",
			version: api.MergeCommitEntryLineageVersion + 1,
			stats:   newStats(objectio.WithCNOrigin()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMergeEntrySource(
				&api.MergeCommitEntry{LineageVersion: test.version},
				test.stats,
			)
			if test.reject {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
				require.ErrorContains(t, err, "retry on a compatible CN")
				return
			}
			require.NoError(t, err)
		})
	}
}
