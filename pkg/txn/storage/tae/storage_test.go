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

package taestorage

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
	"github.com/stretchr/testify/require"
)

func TestNewTAEStorageReturnsTAEOpenError(t *testing.T) {
	rt := runtime.DefaultRuntime()
	opt := &options.Options{SID: rt.ServiceUUID()}
	var err error
	require.NotPanics(t, func() {
		_, err = NewTAEStorage(
			context.Background(),
			"/dev/null/cannot-create-tae-store",
			opt,
			metadata.TNShard{},
			rt,
			"",
			options.NewDefaultLogtailServerCfg(),
			nil,
			nil,
		)
	})
	require.Error(t, err)
}

func TestNewTAEStorageClosesTAEWhenLogtailServerCreationFails(t *testing.T) {
	ctx := context.Background()
	rt := runtime.DefaultRuntime()
	dataDir := t.TempDir()
	expectedErr := errors.New("injected logtail server failure")

	_, err := newTAEStorage(
		ctx,
		dataDir,
		(&options.Options{SID: rt.ServiceUUID()}).FillDefaults(dataDir),
		metadata.TNShard{},
		rt,
		"",
		options.NewDefaultLogtailServerCfg(),
		nil,
		nil,
		func(
			string,
			string,
			*service.LogtailServer,
			...morpc.ServerOption,
		) (morpc.RPCServer, error) {
			return nil, expectedErr
		},
	)
	require.ErrorIs(t, err, expectedErr)

	handle, err := rpc.NewTAEHandle(
		ctx,
		dataDir,
		nil,
		(&options.Options{}).FillDefaults(dataDir),
	)
	require.NoError(t, err)
	require.NoError(t, handle.HandleClose(ctx))
}
