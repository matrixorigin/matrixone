// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
)

func TestWithOption(t *testing.T) {
	ctx := context.Background()
	require.Nil(t, GetBackendOptions(ctx))
	require.Nil(t, GetClientOptions(ctx))

	// set morpc.BackendOption
	backendOpts := []morpc.BackendOption{
		morpc.WithBackendConnectTimeout(time.Second),
	}
	ctx1 := SetBackendOptions(ctx, backendOpts...)
	backendOptsRet := GetBackendOptions(ctx1)
	require.NotNil(t, backendOptsRet)
	require.Equal(t, len(backendOpts), len(backendOptsRet))
	require.Equal(t, backendOpts, backendOptsRet)

	// set morpc.BackendOption again
	newBackendOpts := []morpc.BackendOption{}
	ctx2 := SetBackendOptions(ctx1, newBackendOpts...)
	newBackendOptsRet := GetBackendOptions(ctx2)
	require.NotNil(t, backendOptsRet)
	require.Equal(t, len(newBackendOpts), len(newBackendOptsRet))
	require.Equal(t, newBackendOpts, newBackendOptsRet)

	clientOpts := []morpc.ClientOption{
		morpc.WithClientMaxBackendPerHost(1),
	}
	ctx3 := SetClientOptions(ctx, clientOpts...)
	clientOptsRet := GetClientOptions(ctx3)
	require.NotNil(t, clientOptsRet)
	require.Equal(t, len(clientOpts), len(clientOptsRet))
	require.Equal(t, clientOpts, clientOptsRet)
}
