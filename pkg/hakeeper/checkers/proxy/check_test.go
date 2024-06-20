// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

var expiredTick = uint64(hakeeper.DefaultProxyStoreTimeout / time.Second * hakeeper.DefaultTickPerSecond)

func TestParseProxyStores(t *testing.T) {
	cases := []struct {
		infos       pb.ProxyState
		currentTick uint64

		working []string
		expired []string
	}{
		{
			infos: pb.ProxyState{
				Stores: map[string]pb.ProxyStore{"a": {
					Tick: 0,
				}},
			},
			currentTick: 0,
			working:     []string{"a"},
			expired:     []string{},
		},
		{
			infos: pb.ProxyState{
				Stores: map[string]pb.ProxyStore{"a": {
					Tick: 0,
				}},
			},
			currentTick: expiredTick + 1,
			working:     []string{},
			expired:     []string{"a"},
		},
		{
			infos: pb.ProxyState{
				Stores: map[string]pb.ProxyStore{
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
		working, expired := parseProxyStores(cfg, c.infos, c.currentTick)
		assert.Equal(t, c.working, working)
		assert.Equal(t, c.expired, expired)
	}
}

func TestCheck(t *testing.T) {
	infos := pb.ProxyState{
		Stores: map[string]pb.ProxyStore{"a": {
			Tick: 0,
		}},
	}
	currentTick := expiredTick + 1
	cfg := hakeeper.Config{}
	cfg.Fill()
	pc := NewProxyServiceChecker(
		hakeeper.NewCheckerCommonFields(
			"",
			cfg,
			nil,
			pb.ClusterInfo{},
			pb.TaskTableUser{},
			currentTick,
		),
		infos,
	)
	ops := pc.Check()
	assert.Equal(t, 1, len(ops))
	steps := ops[0].OpSteps()
	assert.Equal(t, 1, len(steps))
	s, ok := steps[0].(operator.DeleteProxyStore)
	assert.True(t, ok)
	assert.NotNil(t, s)
	assert.Equal(t, "a", s.StoreID)
}
