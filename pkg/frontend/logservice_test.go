// Copyright 2021 - 2024 Matrix Origin
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

package frontend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortLogserviceReplicaResult(t *testing.T) {
	type testCase struct {
		infoLists [][]logserviceReplicaInfo
	}
	cases := testCase{
		infoLists: [][]logserviceReplicaInfo{
			{
				{index: 0, hasReplica: true, shardID: 0, replicaID: 131074, replicaRole: Leader},
				{index: 3, hasReplica: true, shardID: 1, replicaID: 262147, replicaRole: Leader},
				{index: 1, hasReplica: true, shardID: 0, replicaID: 131072, replicaRole: Follower},
				{index: 2, hasReplica: true, shardID: 0, replicaID: 131073, replicaRole: Follower},
				{index: 4, hasReplica: true, shardID: 1, replicaID: 262145, replicaRole: Follower},
				{index: 5, hasReplica: true, shardID: 1, replicaID: 262146, replicaRole: Follower},
			},
			{
				{index: 0, hasReplica: true, shardID: 0, replicaID: 131074, replicaRole: Leader},
				{index: 6, hasReplica: false},
				{index: 1, hasReplica: true, shardID: 0, replicaID: 131072, replicaRole: Follower},
				{index: 2, hasReplica: true, shardID: 0, replicaID: 131073, replicaRole: Follower},
				{index: 3, hasReplica: true, shardID: 1, replicaID: 262145, replicaRole: Leader},
				{index: 4, hasReplica: true, shardID: 1, replicaID: 262146, replicaRole: Follower},
				{index: 5, hasReplica: true, shardID: 1, replicaID: 262148, replicaRole: Follower},
			},
		},
	}
	for _, c := range cases.infoLists {
		infoList := sortLogserviceReplicasResult(c)
		for i, info := range infoList {
			assert.Equal(t, i, info.index)
		}
	}
}

func TestSortLogserviceStoreResult(t *testing.T) {
	type testCase struct {
		infoLists [][]logserviceStoreInfo
	}
	cases := testCase{
		infoLists: [][]logserviceStoreInfo{
			{
				{index: 0, storeID: "0"},
				{index: 2, storeID: "2"},
				{index: 4, storeID: "4"},
				{index: 8, storeID: "8"},
				{index: 1, storeID: "1"},
				{index: 6, storeID: "6"},
				{index: 5, storeID: "5"},
				{index: 7, storeID: "7"},
				{index: 3, storeID: "3"},
			},
		},
	}
	for _, c := range cases.infoLists {
		infoList := sortLogserviceStoresResult(c)
		for i, info := range infoList {
			assert.Equal(t, i, info.index)
		}
	}
}
