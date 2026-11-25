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

package ioutil

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestPipelinePrefetchKeepsCacheTargetsSeparate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		secondPolicy fileservice.Policy
		wantPolicies []fileservice.Policy
	}{
		{name: "baseline_same_cache_target", wantPolicies: []fileservice.Policy{0}},
		{
			name:         "same_object_two_cache_targets",
			secondPolicy: fileservice.IndexFileObject,
			wantPolicies: []fileservice.Policy{0, fileservice.IndexFileObject},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := types.Objectid{2}
			stats := objectio.NewObjectStatsWithObjectID(&id, false, true, true)
			block := stats.ConstructBlockInfo(0)
			loc := block.MetaLocation()
			merged := mergePrefetch([]PrefetchParams{
				{key: loc, typ: PrefetchFileType},
				{key: loc, typ: PrefetchFileType, policy: tc.secondPolicy},
			})
			policies := make([]fileservice.Policy, 0, len(merged))
			for _, req := range merged {
				policies = append(policies, req.policy)
			}
			require.ElementsMatch(t, tc.wantPolicies, policies, "the pipeline schedules one job per cache target")
		})
	}
}
