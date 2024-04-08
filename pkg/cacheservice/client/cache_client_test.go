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

package client

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/cache"
	"github.com/stretchr/testify/assert"
)

func testCreateCacheClient(t *testing.T) CacheClient {
	ct, err := NewCacheClient(ClientConfig{
		RPC: morpc.Config{},
	})
	assert.NoError(t, err)
	return ct
}

func TestNewCacheClient(t *testing.T) {
	ct := testCreateCacheClient(t)
	assert.NotNil(t, ct)
}

func TestUnwrapResponseError(t *testing.T) {
	ct := testCreateCacheClient(t)
	assert.NotNil(t, ct)
	client, ok := ct.(*cacheClient)
	assert.True(t, ok)
	resp1 := &cache.CacheResponse{Response: cache.Response{Error: nil}}
	resp2, err := client.unwrapResponseError(resp1)
	assert.Nil(t, err)
	assert.Equal(t, resp2, resp1)

	e := moerr.NewInternalErrorNoCtx("test")
	moe, err := e.MarshalBinary()
	assert.NoError(t, err)
	resp1 = &cache.CacheResponse{Response: cache.Response{Error: moe}}
	resp2, err = client.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Nil(t, resp2)
}
