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

package dnservice

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
)

func TestCreateLogServiceClient(t *testing.T) {
	s := &store{cfg: &Config{}, stopper: stopper.NewStopper("")}
	s.options.logServiceClientFactory = func(d metadata.DNShard) (logservice.Client, error) {
		return mem.NewMemLog(), nil
	}
	v, err := s.createLogServiceClient(metadata.DNShard{})
	assert.NoError(t, err)
	assert.NotNil(t, v)
}

func TestCreateTxnStorage(t *testing.T) {
	ctx := context.TODO()
	s := &store{rt: runtime.DefaultRuntime(), cfg: &Config{}, stopper: stopper.NewStopper("")}
	s.options.logServiceClientFactory = func(d metadata.DNShard) (logservice.Client, error) {
		return mem.NewMemLog(), nil
	}

	s.cfg.Txn.Storage.Backend = StorageMEMKV
	v, err := s.createTxnStorage(ctx, metadata.DNShard{})
	assert.NoError(t, err)
	assert.NotNil(t, v)

	s.cfg.Txn.Storage.Backend = "error"
	v, err = s.createTxnStorage(ctx, metadata.DNShard{})
	assert.Error(t, err)
	assert.Nil(t, v)
}
