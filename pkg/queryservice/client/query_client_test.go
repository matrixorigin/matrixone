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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/assert"
)

func testCreateQueryClient(t *testing.T) QueryClient {
	ct, err := NewQueryClient("", morpc.Config{})
	assert.NoError(t, err)
	return ct
}

func TestMongoDBClientRetireRequiresProtocolVersion5(t *testing.T) {
	assert.Equal(t, defines.MORPCVersion5, methodVersions[query.CmdMethod_MongoDBClientRetire])
}

// The name says 54 because that is the contract: the protocol RefreshSessionAuth shipped with.
func TestRefreshSessionAuthRequiresProtocolVersion54(t *testing.T) {
	// A method's entry in methodVersions records the protocol it SHIPPED with, not whatever is
	// newest -- nearly every other entry is MORPCVersion1 for that reason. Gating a method on
	// the latest version would make it unusable until every peer in the cluster had upgraded,
	// which is the opposite of what the gate is for. So this stays at 54 however far the
	// protocol moves on.
	//
	// The assertion that pinned the latest version literally lived here too. Both this branch
	// and main had to edit it this cycle, for unrelated features, which is the argument
	// against it: TestMethodVersionsNeverExceedTheLatestProtocol checks the property it was
	// reaching for and needs no edit when the version moves.
	assert.Equal(t, defines.MORPCVersion54, methodVersions[query.CmdMethod_RefreshSessionAuth])
}

// No method may require a protocol NEWER than the newest one that exists: no peer could ever
// satisfy such a gate, so the RPC would be permanently unreachable rather than merely gated.
//
// This replaces an assertion that pinned the literal latest version. That one fired on every
// bump, including bumps by features with nothing to do with session auth, and the edit it
// demanded was always the same: move the literal. Checking the relationship instead keeps the
// property it was reaching for -- a gate that cannot outrun the protocol -- without needing an
// edit each time, and covers every method rather than one.
func TestMethodVersionsNeverExceedTheLatestProtocol(t *testing.T) {
	for method, version := range methodVersions {
		assert.LessOrEqual(t, version, defines.MORPCLatestVersion,
			"%s is gated on protocol %d, above the latest %d: no peer can satisfy it",
			method, version, defines.MORPCLatestVersion)
	}
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

	resp1 = &query.Response{
		CmdMethod: query.CmdMethod_RefreshSessionAuth,
		Error:     moe,
		RefreshSessionAuthResponse: &query.RefreshSessionAuthResponse{
			AuthenticationFailed: true,
		},
	}
	resp2, err = client.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Same(t, resp1, resp2)

	resp1 = &query.Response{
		CmdMethod: query.CmdMethod_RefreshSessionAuth,
		Error:     moe,
		RefreshSessionAuthResponse: &query.RefreshSessionAuthResponse{
			RequestRejected: true,
		},
	}
	resp2, err = client.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Same(t, resp1, resp2)
}
