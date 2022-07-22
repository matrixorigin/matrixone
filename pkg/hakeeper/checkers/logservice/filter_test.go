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

package logservice

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelector(t *testing.T) {
	cases := []struct {
		shardInfo logservice.LogShardInfo
		stores    *util.ClusterStores
		expected  util.StoreID
	}{
		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: &util.ClusterStores{
				Working: []*util.Store{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}},
			},
			expected: "d",
		},
		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: &util.ClusterStores{
				Working: []*util.Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
				Expired: []*util.Store{{ID: "d"}},
			},
			expected: "",
		},
		{
			shardInfo: logservice.LogShardInfo{
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			},
			stores: &util.ClusterStores{
				Working: []*util.Store{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "e"}, {ID: "hello"}, {ID: "d"}},
			},
			expected: "d",
		},
	}

	for _, c := range cases {
		output := selectStore(c.shardInfo, c.stores)
		assert.Equal(t, c.expected, output)
	}
}
