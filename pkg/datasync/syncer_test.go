// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/stretchr/testify/assert"
)

func createTestDataSync(
	t *testing.T,
	rt runtime.Runtime,
	stopper *stopper.Stopper,
	fs fileservice.FileService,
) logservice.DataSync {
	ds, err := NewDataSync(
		"",
		stopper,
		rt,
		logservice.HAKeeperClientConfig{},
		fs,
	)
	assert.NoError(t, err)
	assert.NotNil(t, ds)
	return ds
}

func TestSyncer_Create(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rt := runtime.DefaultRuntime()
	st := stopper.NewStopper(
		"test",
		stopper.WithLogger(rt.Logger().RawLogger()),
	)
	defer st.Stop()

	t.Run("create consumer error", func(t *testing.T) {
		assert.Panics(t, func() {
			createTestDataSync(t, rt, st, nil)
		})
	})

	t.Run("ok", func(t *testing.T) {
		s := createTestDataSync(t, rt, st, newTestFS())
		assert.NotNil(t, s)
		assert.NoError(t, s.Close())
	})
}

func TestSyncer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rt := runtime.DefaultRuntime()
	st := stopper.NewStopper(
		"test",
		stopper.WithLogger(rt.Logger().RawLogger()),
	)
	defer st.Stop()

	t.Run("ok", func(t *testing.T) {
		s := createTestDataSync(t, rt, st, newTestFS())
		assert.NotNil(t, s)
		defer func() {
			assert.NoError(t, s.Close())
		}()
		ctx := context.Background()
		s.Append(ctx, 1, []byte("test"))
	})
}

func TestSyncer_NotifyReplicaID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rt := runtime.DefaultRuntime()
	st := stopper.NewStopper(
		"test",
		stopper.WithLogger(rt.Logger().RawLogger()),
	)
	defer st.Stop()

	s := createTestDataSync(t, rt, st, newTestFS())
	assert.NotNil(t, s)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	s.NotifyReplicaID(1, 100, logservice.AddReplica)
	ss := s.(*syncer)
	v, ok := ss.common.shardReplicaID.Load(uint64(1))
	assert.True(t, ok)
	assert.Equal(t, uint64(100), v.(uint64))

	v, ok = ss.common.shardReplicaID.Load(uint64(2))
	assert.False(t, ok)
	assert.Nil(t, v)

	s.NotifyReplicaID(1, 100, logservice.DelReplica)
	v, ok = ss.common.shardReplicaID.Load(uint64(1))
	assert.False(t, ok)
	assert.Nil(t, v)
}
