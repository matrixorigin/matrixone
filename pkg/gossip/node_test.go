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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
)

func getRandomPort() int {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	return rand.Intn(65535-1024) + 1024
}

func TestNodeGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	svcPort1 := getRandomPort()
	cachePort1 := getRandomPort()
	n1, err := NewNode(ctx, "n1",
		WithListenAddrFn(func() string {
			return fmt.Sprintf("127.0.0.1:%d", svcPort1)
		}),
		WithServiceAddrFn(func() string {
			return fmt.Sprintf("127.0.0.1:%d", svcPort1)
		}),
		WithCacheServerAddrFn(func() string {
			return fmt.Sprintf("127.0.0.1:%d", cachePort1)
		}))
	assert.NoError(t, err)
	assert.NotNil(t, n1)
	err = n1.Create()
	if err != nil {
		return
	}
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, n1.Leave(time.Millisecond*300))
	}()
	err = n1.Join(nil)
	assert.Equal(t, 1, n1.NumMembers())
	assert.NotNil(t, n1.DistKeyCacheGetter()())
	assert.NoError(t, err)

	svcPort2 := getRandomPort()
	cachePort2 := getRandomPort()
	n2, err := NewNode(ctx, "n2",
		WithListenAddrFn(func() string {
			return fmt.Sprintf("127.0.0.1:%d", svcPort2)
		}),
		WithServiceAddrFn(func() string {
			return fmt.Sprintf("127.0.0.1:%d", svcPort2)
		}),
		WithCacheServerAddrFn(func() string {
			return fmt.Sprintf("127.0.0.1:%d", cachePort2)
		}))
	assert.NoError(t, err)
	assert.NotNil(t, n2)
	err = n2.Create()
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, n2.Leave(time.Millisecond*300))
	}()
	err = n2.Join([]string{fmt.Sprintf("127.0.0.1:%d", svcPort1)})
	assert.NoError(t, err)
	assert.Equal(t, 2, n2.NumMembers())
	assert.NotNil(t, n2.DistKeyCacheGetter()())
}
