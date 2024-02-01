// Copyright 2022 Matrix Origin
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

package task

import (
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

var expiredTick = uint64(hakeeper.DefaultCNStoreTimeout / time.Second * hakeeper.DefaultTickPerSecond)

func TestSelectWorkingCNs(t *testing.T) {
	cases := []struct {
		infos       pb.CNState
		currentTick uint64
		expectedCN  map[string]struct{}
	}{
		{
			infos:       pb.CNState{},
			currentTick: 0,
			expectedCN:  map[string]struct{}{},
		},
		{
			infos:       pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {Tick: 0}}},
			currentTick: 0,
			expectedCN:  map[string]struct{}{"a": {}},
		},
		{
			infos: pb.CNState{Stores: map[string]pb.CNStoreInfo{
				"a": {Tick: 0},
				"b": {Tick: expiredTick}}},
			currentTick: expiredTick + 1,
			expectedCN:  map[string]struct{}{"b": {}},
		},
	}

	for _, c := range cases {
		cfg := hakeeper.Config{}
		cfg.Fill()
		working := selectCNs(c.infos, notExpired(cfg, c.currentTick))
		assert.Equal(t, c.expectedCN, getUUIDs(working))
	}
}

func TestContainsLabel(t *testing.T) {
	cases := []struct {
		infos      pb.CNState
		key, value string
		expectedCN map[string]struct{}
	}{
		{
			infos: pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}}},
			key:   "k1", value: "v1",
			expectedCN: map[string]struct{}{},
		},
		{
			infos: pb.CNState{Stores: map[string]pb.CNStoreInfo{
				"a": {Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v1"}}}},
			}},
			key: "k1", value: "v1",
			expectedCN: map[string]struct{}{"a": {}},
		},
		{
			infos: pb.CNState{
				Stores: map[string]pb.CNStoreInfo{
					"a": {
						Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v1", "v2"}}},
					},
					"b": {
						Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v1"}}},
					},
				},
			},
			key: "k1", value: "v1",
			expectedCN: map[string]struct{}{"a": {}, "b": {}},
		},
		{
			infos: pb.CNState{
				Stores: map[string]pb.CNStoreInfo{
					"a": {
						Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v1", "v2"}}},
					},
					"b": {
						Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v1"}}},
					},
				},
			},
			key: "k1", value: "v2",
			expectedCN: map[string]struct{}{"a": {}},
		},
		{
			infos: pb.CNState{
				Stores: map[string]pb.CNStoreInfo{
					"a": {
						Labels: map[string]metadata.LabelList{
							"k1": {Labels: []string{"v1"}},
						},
					},
				},
			},
			key: "k2", value: "v1",
			expectedCN: map[string]struct{}{},
		},
	}

	for _, c := range cases {
		cfg := hakeeper.Config{}
		cfg.Fill()
		working := selectCNs(c.infos, containsLabel(c.key, c.value))
		assert.Equal(t, c.expectedCN, getUUIDs(working))
	}
}

func TestWithResource(t *testing.T) {
	cases := []struct {
		infos      pb.CNState
		cpu, mem   uint64
		expectedCN map[string]struct{}
	}{
		{
			infos:      pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}}},
			cpu:        1,
			expectedCN: map[string]struct{}{},
		},
		{
			infos: pb.CNState{Stores: map[string]pb.CNStoreInfo{
				"a": {Resource: pb.Resource{
					CPUTotal: 1,
					MemTotal: 100,
				}},
			}},
			cpu: 1, mem: 100,
			expectedCN: map[string]struct{}{"a": {}},
		},
		{
			infos: pb.CNState{
				Stores: map[string]pb.CNStoreInfo{
					"a": {Resource: pb.Resource{
						CPUTotal: 1,
						MemTotal: 100,
					}},
					"b": {
						Resource: pb.Resource{
							CPUTotal: 2,
							MemTotal: 200,
						},
					},
				},
			},
			cpu: 2, mem: 150,
			expectedCN: map[string]struct{}{"b": {}},
		},
	}

	for _, c := range cases {
		cfg := hakeeper.Config{}
		cfg.Fill()
		working := selectCNs(c.infos, withCPU(c.cpu), withMemory(c.mem))
		assert.Equal(t, c.expectedCN, getUUIDs(working))
	}
}

func TestContains(t *testing.T) {
	assert.True(t, contains([]string{"a"}, "a"))
	assert.False(t, contains([]string{"a"}, "b"))
}
