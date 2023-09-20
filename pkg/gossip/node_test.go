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
	"context"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestNodeGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	n1, err := NewNode(ctx, "n1",
		WithListenAddrFn(func() string {
			return "127.0.0.1:38001"
		}),
		WithServiceAddrFn(func() string {
			return "127.0.0.1:38001"
		}),
		WithCacheServerAddrFn(func() string {
			return "127.0.0.1:38101"
		}))
	assert.NoError(t, err)
	assert.NotNil(t, n1)
	err = n1.Create()
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, n1.Leave(time.Millisecond*300))
	}()
	err = n1.Join(nil)
	assert.Equal(t, 1, n1.NumMembers())
	assert.NotNil(t, n1.DistKeyCacheGetter()())

	assert.NoError(t, err)
	n2, err := NewNode(ctx, "n2",
		WithListenAddrFn(func() string {
			return "127.0.0.1:38002"
		}),
		WithServiceAddrFn(func() string {
			return "127.0.0.1:38002"
		}),
		WithCacheServerAddrFn(func() string {
			return "127.0.0.1:38201"
		}))
	assert.NoError(t, err)
	assert.NotNil(t, n2)
	err = n2.Create()
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, n2.Leave(time.Millisecond*300))
	}()
	err = n2.Join([]string{"127.0.0.1:38001"})
	assert.NoError(t, err)
	assert.Equal(t, 2, n2.NumMembers())
	assert.NotNil(t, n2.DistKeyCacheGetter()())
}
