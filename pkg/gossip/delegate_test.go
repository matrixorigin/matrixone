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

package gossip

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/cache"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestDelegate_NodeMeta(t *testing.T) {
	d := newDelegate(&zap.Logger{}, "127.0.0.1:8889")
	m := d.NodeMeta(100)
	assert.NotNil(t, m)
	assert.Equal(t, "127.0.0.1:8889", string(m))
}

func TestDelegate_GetBroadcastsAndNotify(t *testing.T) {
	d := newDelegate(&zap.Logger{}, "127.0.0.1:8889")
	data := d.GetBroadcasts(4, 32*1024)
	assert.Nil(t, data)

	for i := 0; i < 10; i++ {
		ck := cache.CacheKey{
			Path:   fmt.Sprintf("p%d", i),
			Offset: int64(10 * i),
			Sz:     int64(10 * i),
		}
		d.getDistKeyCache().AddItem(ck, gossip.Operation_Set)
		assert.Equal(t, i+1, len(d.distKeyCache.queueMu.itemQueue))
	}

	data = d.GetBroadcasts(4, 32*1024)
	assert.NotNil(t, data)

	t.Run("self", func(t *testing.T) {
		for _, single := range data {
			d.NotifyMsg(single)
		}
		assert.Equal(t, 10, len(d.distKeyCache.cacheMu.keyTarget))
		for i := 0; i < 15; i++ {
			target := d.getDistKeyCache().Target(cache.CacheKey{
				Path:   fmt.Sprintf("p%d", i),
				Offset: int64(10 * i),
				Sz:     int64(10 * i),
			})
			assert.Equal(t, "", target)
		}
	})

	t.Run("other", func(t *testing.T) {
		d1 := newDelegate(&zap.Logger{}, "127.0.0.1:7779")
		for _, single := range data {
			d1.NotifyMsg(single)
		}
		assert.Equal(t, 10, len(d1.distKeyCache.cacheMu.keyTarget))
		for i := 0; i < 15; i++ {
			target := d1.getDistKeyCache().Target(cache.CacheKey{
				Path:   fmt.Sprintf("p%d", i),
				Offset: int64(10 * i),
				Sz:     int64(10 * i),
			})
			if i < 10 {
				assert.Equal(t, "127.0.0.1:8889", target)
			} else {
				assert.Equal(t, "", target)
			}
		}
	})
}

func TestDelegate_LocalStateAndMergeRemoteState(t *testing.T) {
	d1 := newDelegate(&zap.Logger{}, "127.0.0.1:8888")
	d2 := newDelegate(&zap.Logger{}, "127.0.0.1:8889")

	for i := 0; i < 10; i++ {
		ck := cache.CacheKey{
			Path:   fmt.Sprintf("p%d", i),
			Offset: int64(10 * i),
			Sz:     int64(10 * i),
		}
		d1.getDistKeyCache().AddItem(ck, gossip.Operation_Set)
		assert.Equal(t, i+1, len(d1.distKeyCache.queueMu.itemQueue))
	}

	data := d1.GetBroadcasts(4, 32*1024)
	assert.NotNil(t, data)

	for _, single := range data {
		d2.NotifyMsg(single)
	}
	assert.Equal(t, 10, len(d2.distKeyCache.cacheMu.keyTarget))

	buf := d2.LocalState(false)
	d2.MergeRemoteState(buf, true)
	for i := 0; i < 15; i++ {
		target := d2.getDistKeyCache().Target(cache.CacheKey{
			Path:   fmt.Sprintf("p%d", i),
			Offset: int64(10 * i),
			Sz:     int64(10 * i),
		})
		if i < 10 {
			assert.Equal(t, "127.0.0.1:8888", target)
		} else {
			assert.Equal(t, "", target)
		}
	}
}
