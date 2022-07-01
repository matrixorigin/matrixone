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

package bootstrap

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewBootstrapManager(t *testing.T) {
	cases := []struct {
		cluster  pb.ClusterInfo
		expected *Manager
	}{
		{
			cluster: pb.ClusterInfo{DNShards: nil, LogShards: nil},
			expected: &Manager{
				cluster: pb.ClusterInfo{DNShards: nil, LogShards: nil},
			},
		},
		{
			cluster: pb.ClusterInfo{
				DNShards: []metadata.DNShardRecord{{
					ShardID:    1,
					LogShardID: 1,
				}},
				LogShards: []metadata.LogShardRecord{{ShardID: 2}},
			},
			expected: &Manager{
				cluster: pb.ClusterInfo{
					DNShards: []metadata.DNShardRecord{{
						ShardID:    1,
						LogShardID: 1,
					}},
					LogShards: []metadata.LogShardRecord{{ShardID: 2}},
				},
			},
		},
	}

	for _, c := range cases {
		bm := NewBootstrapManager(c.cluster)
		assert.Equal(t, c.expected, bm)
	}
}

func TestBootstrap(t *testing.T) {
	cases := []struct {
		desc string

		cluster pb.ClusterInfo
		dn      pb.DNState
		log     pb.LogState

		expectedNum            int
		expectedInitialMembers map[uint64]string
		err                    error
	}{
		{
			desc: "1 log shard with 3 replicas and 1 dn shard",

			cluster: pb.ClusterInfo{
				DNShards: []metadata.DNShardRecord{{ShardID: 1, LogShardID: 1}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			dn: pb.DNState{
				Stores: map[string]pb.DNStoreInfo{"dn-a": {}},
			},
			log: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{
					"log-a": {Tick: 100},
					"log-b": {Tick: 110},
					"log-c": {Tick: 120},
					"log-d": {Tick: 130},
				},
			},

			expectedNum: 4,
			expectedInitialMembers: map[uint64]string{
				1: "log-d",
				2: "log-c",
				3: "log-b",
			},
		},
		{
			desc: "err: not enough log stores",

			cluster: pb.ClusterInfo{
				DNShards: []metadata.DNShardRecord{{ShardID: 1, LogShardID: 1}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			dn: pb.DNState{
				Stores: map[string]pb.DNStoreInfo{"dn-a": {}},
			},
			log: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{
					"log-a": {Tick: 100},
					"log-b": {Tick: 110}},
			},

			expectedNum:            0,
			expectedInitialMembers: map[uint64]string{},
			err:                    errors.New("not enough log stores"),
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)

		alloc := util.NewTestIDAllocator(0)
		bm := NewBootstrapManager(c.cluster)
		output, err := bm.Bootstrap(alloc, c.dn, c.log)
		assert.Equal(t, c.err, err)
		if err != nil {
			continue
		}
		assert.Equal(t, c.expectedNum, len(output))
		assert.Equal(t, c.expectedInitialMembers, output[0].ConfigChange.InitialMembers)
	}
}

func TestCheckBootstrap(t *testing.T) {
	cases := []struct {
		desc string

		cluster pb.ClusterInfo
		log     pb.LogState

		expected bool
	}{
		{
			desc: "successfully started",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 3},
					{ShardID: 2, NumberOfReplicas: 3},
					{ShardID: 3, NumberOfReplicas: 3},
				},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b"},
					},
					2: {
						ShardID:  2,
						Replicas: map[uint64]string{1: "a", 3: "c"},
					},
					3: {
						ShardID:  3,
						Replicas: map[uint64]string{2: "b", 3: "c"},
					},
				},
			},
			expected: true,
		},
		{
			desc: "shard 1 not started",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 3},
					{ShardID: 2, NumberOfReplicas: 3},
					{ShardID: 3, NumberOfReplicas: 3},
				},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a"},
					},
					2: {
						ShardID:  2,
						Replicas: map[uint64]string{1: "a", 3: "c"},
					},
					3: {
						ShardID:  3,
						Replicas: map[uint64]string{2: "b", 3: "c"},
					},
				},
			},
			expected: false,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		bm := NewBootstrapManager(c.cluster)
		output := bm.CheckBootstrap(c.log)
		assert.Equal(t, c.expected, output)
	}
}

func TestSortLogStores(t *testing.T) {
	cases := []struct {
		logStores map[string]pb.LogStoreInfo
		expected  []string
	}{{
		logStores: map[string]pb.LogStoreInfo{
			"a": {Tick: 100},
			"b": {Tick: 120},
			"c": {Tick: 90},
			"d": {Tick: 95},
		},
		expected: []string{"b", "a", "d", "c"},
	}}

	for _, c := range cases {
		output := sortLogStoresByTick(c.logStores)
		assert.Equal(t, c.expected, output)
	}
}

func TestSortDNStores(t *testing.T) {
	cases := []struct {
		dnStores map[string]pb.DNStoreInfo
		expected []string
	}{{
		dnStores: map[string]pb.DNStoreInfo{
			"a": {Tick: 100},
			"b": {Tick: 120},
			"c": {Tick: 90},
			"d": {Tick: 95},
		},
		expected: []string{"b", "a", "d", "c"},
	}}

	for _, c := range cases {
		output := sortDNStoresByTick(c.dnStores)
		assert.Equal(t, c.expected, output)
	}
}
