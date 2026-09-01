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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/assert"
)

func testCreateQueryClient(t *testing.T) QueryClient {
	ct, err := NewQueryClient("", morpc.Config{})
	assert.NoError(t, err)
	return ct
}

func TestMethodProtocolVersions(t *testing.T) {
	assert.Equal(t, defines.MORPCVersion5, methodVersions[query.CmdMethod_MongoDBClientRetire])
	assert.Equal(t, defines.MORPCVersion43, methodVersions[query.CmdMethod_Fulltext2CacheFence])
}

func TestFulltext2CacheFenceRejectsV42AndAcceptsV43(t *testing.T) {
	const service = "fulltext2-fence-version-test"
	moruntime.RunTest(service, func(rt moruntime.Runtime) {
		req := &query.Request{CmdMethod: query.CmdMethod_Fulltext2CacheFence}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion42)
		assert.ErrorContains(t, checkMethodVersion(context.Background(), service, req), "unsupported protocol version 43")
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion43)
		assert.NoError(t, checkMethodVersion(context.Background(), service, req))
	})
}

func TestNewCacheClient(t *testing.T) {
	ct := testCreateQueryClient(t)
	assert.NotNil(t, ct)
}

func TestUnwrapResponseError(t *testing.T) {
	ct := testCreateQueryClient(t)
	assert.NotNil(t, ct)
	client, ok := ct.(*queryClient)
	assert.True(t, ok)
	resp1 := &query.Response{Error: nil}
	resp2, err := client.unwrapResponseError(resp1)
	assert.Nil(t, err)
	assert.Equal(t, resp2, resp1)

	e := moerr.NewInternalErrorNoCtx("test")
	moe, err := e.MarshalBinary()
	assert.NoError(t, err)
	resp1 = &query.Response{Error: moe}
	resp2, err = client.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Nil(t, resp2)
}
