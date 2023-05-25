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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestErrorCode_String(t *testing.T) {
	c := codeNone
	require.NotEqual(t, "", c.String())
	c = codeAuthFailed
	require.NotEqual(t, "", c.String())
	c = codeClientDisconnect
	require.NotEqual(t, "", c.String())
	c = codeServerDisconnect
	require.NotEqual(t, "", c.String())
}

func TestErrorCode_WithCode(t *testing.T) {
	c := withCode(nil, codeNone)
	require.Nil(t, c)
	c = withCode(moerr.NewInternalErrorNoCtx("error1"), codeNone)
	require.Equal(t, "internal error: error1", c.Error())
	c = withCode(moerr.NewInternalErrorNoCtx("error2"), codeAuthFailed)
	require.Equal(t, "Auth failed: internal error: error2", c.Error())
	c = withCode(moerr.NewInternalErrorNoCtx("error3"), codeClientDisconnect)
	require.Equal(t, "Client disconnect: internal error: error3", c.Error())
	c = withCode(moerr.NewInternalErrorNoCtx("error4"), codeServerDisconnect)
	require.Equal(t, "Server disconnect: internal error: error4", c.Error())
}

func TestRetryableError(t *testing.T) {
	e := newConnectErr(nil)
	require.True(t, isRetryableErr(e))

	e = newConnectErr(moerr.NewInternalErrorNoCtx("e"))
	require.Equal(t, e.Error(), "internal error: e")
}
