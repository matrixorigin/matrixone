// Copyright 2021 - 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

func TestLogtailMerge(t *testing.T) {
	var cbValue int
	tail1 := &LogtailPhase{
		tail: logtail.TableLogtail{
			CkpLocation: "aaa;bbb;",
			Ts: &timestamp.Timestamp{
				PhysicalTime: 100,
			},
			Table: &api.TableID{
				DbId: 1,
				TbId: 2,
			},
			Commands: []api.Entry{{TableId: 2}},
		},
		closeCB: func() {
			cbValue++
		},
	}
	tail2 := &LogtailPhase{
		tail: logtail.TableLogtail{
			CkpLocation: "ccc;ddd",
			Ts: &timestamp.Timestamp{
				PhysicalTime: 200,
			},
			Table: &api.TableID{
				DbId: 1,
				TbId: 2,
			},
			Commands: []api.Entry{{TableId: 2}},
		},
		closeCB: func() {
			cbValue++
		},
	}
	lm := newLogtailMerger(tail1, tail2)
	assert.NotNil(t, lm)
	tail, cb := lm.Merge()
	cb()
	assert.Equal(t, 2, cbValue)
	assert.Equal(t, 2, len(tail.Commands))
	assert.Equal(t, int64(200), tail.Ts.PhysicalTime)
	assert.Equal(t, "aaa;bbb;ccc;ddd", tail.CkpLocation)
}
