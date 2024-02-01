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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

var expiredTick = uint64(hakeeper.DefaultCNStoreTimeout / time.Second * hakeeper.DefaultTickPerSecond)

func TestSelectWorkingCNs(t *testing.T) {
	cases := []struct {
		infos           pb.CNState
		currentTick     uint64
		expectedWorking []string
	}{
		{
			infos:           pb.CNState{},
			currentTick:     0,
			expectedWorking: []string(nil),
		},
		{
			infos:           pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {Tick: 0}}},
			currentTick:     0,
			expectedWorking: []string{"a"},
		},
		{
			infos: pb.CNState{Stores: map[string]pb.CNStoreInfo{
				"a": {Tick: 0},
				"b": {Tick: expiredTick}}},
			currentTick:     expiredTick + 1,
			expectedWorking: []string{"b"},
		},
	}

	for _, c := range cases {
		cfg := hakeeper.Config{}
		cfg.Fill()
		working := selectCNs(c.infos, notExpired(cfg, c.currentTick))
		var uuids []string
		if len(working.Stores) != 0 {
			uuids = make([]string, 0, len(working.Stores))
			for uuid := range working.Stores {
				uuids = append(uuids, uuid)
			}
		}
		assert.Equal(t, c.expectedWorking, uuids)
	}
}

func TestContains(t *testing.T) {
	assert.True(t, contains([]string{"a"}, "a"))
	assert.False(t, contains([]string{"a"}, "b"))
}
