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

package cnservice

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var expiredTick = uint64(hakeeper.DefaultCNStoreTimeout / time.Second * hakeeper.DefaultTickPerSecond)

func TestParseCNStores(t *testing.T) {
	cases := []struct {
		infos       pb.CNState
		currentTick uint64

		working []string
		expired []string
	}{
		{
			infos: pb.CNState{
				Stores: map[string]pb.CNStoreInfo{"a": {
					Tick: 0,
				}},
			},
			currentTick: 0,
			working:     []string{"a"},
			expired:     []string{},
		},
		{
			infos: pb.CNState{
				Stores: map[string]pb.CNStoreInfo{"a": {
					Tick: 0,
				}},
			},
			currentTick: expiredTick + 1,
			working:     []string{},
			expired:     []string{"a"},
		},
		{
			infos: pb.CNState{
				Stores: map[string]pb.CNStoreInfo{
					"a": {
						Tick: expiredTick,
					},
					"b": {
						Tick: 0,
					},
				},
			},
			currentTick: expiredTick + 1,
			working:     []string{"a"},
			expired:     []string{"b"},
		},
	}

	cfg := hakeeper.Config{}
	cfg.Fill()

	for _, c := range cases {
		working, expired := parseCNStores(cfg, c.infos, c.currentTick)
		assert.Equal(t, c.working, working)
		assert.Equal(t, c.expired, expired)
	}
}
