// Copyright 2022 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExcludedFilter(t *testing.T) {
	cases := []struct {
		inputs   []*util.Store
		excluded []string
		expected []*util.Store
	}{
		{
			inputs:   []*util.Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a"},
			expected: []*util.Store{{ID: "b"}, {ID: "c"}},
		},
		{
			inputs:   []*util.Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a", "b"},
			expected: []*util.Store{{ID: "c"}},
		},
		{
			inputs:   []*util.Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a", "b", "c"},
			expected: nil,
		},
		{
			inputs:   []*util.Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"A", "B", "C"},
			expected: []*util.Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
		},
	}

	for _, c := range cases {
		outputs := util.FilterStore(c.inputs, []util.IFilter{newExcludedFilter(c.excluded...)})
		assert.Equal(t, c.expected, outputs)
	}
}

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
		output := selector(c.shardInfo, c.stores)
		assert.Equal(t, c.expected, output)
	}
}
