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

package tnservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateLogServiceClient(t *testing.T) {
	s := &store{cfg: &Config{}, stopper: stopper.NewStopper("")}
	s.options.logServiceClientFactory = func(d metadata.TNShard) (logservice.Client, error) {
		return mem.NewMemLog(), nil
	}
	v, err := s.createLogServiceClient(metadata.TNShard{})
	assert.NoError(t, err)
	assert.NotNil(t, v)
}

func TestWALClientFactoryUsesCallerCancellation(t *testing.T) {
	s := &store{cfg: &Config{}}
	s.cfg.LogService.ConnectTimeout.Duration = time.Minute
	shard := metadata.TNShard{LogShardID: 1, ReplicaID: 1}
	factory := s.createWALContextClientFactory(shard)
	require.NotNil(t, factory)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := factory(ctx)
	require.Nil(t, client)
	require.ErrorIs(t, err, context.Canceled)

	s.options.logServiceClientFactory = func(metadata.TNShard) (logservice.Client, error) {
		return mem.NewMemLog(), nil
	}
	require.Nil(t, s.createWALContextClientFactory(shard))
}

func TestCreateTxnStorage(t *testing.T) {
	ctx := context.TODO()
	s := &store{rt: runtime.DefaultRuntime(), cfg: &Config{}, stopper: stopper.NewStopper("")}
	s.options.logServiceClientFactory = func(d metadata.TNShard) (logservice.Client, error) {
		return mem.NewMemLog(), nil
	}

	s.cfg.Txn.Storage.Backend = StorageMEMKV
	v, err := s.createTxnStorage(ctx, metadata.TNShard{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, v)

	s.cfg.Txn.Storage.Backend = "error"
	v, err = s.createTxnStorage(ctx, metadata.TNShard{}, nil)
	assert.Error(t, err)
	assert.Nil(t, v)
}
