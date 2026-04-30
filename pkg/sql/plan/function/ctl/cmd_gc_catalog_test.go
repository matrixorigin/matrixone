// Copyright 2024 Matrix Origin
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

package ctl

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockGCEngine struct {
	engine.Engine
	called bool
	ago    time.Duration
}

func (m *mockGCEngine) GCCatalogCache(_ context.Context, ago time.Duration) {
	m.called = true
	m.ago = ago
}

func TestHandleGCCatalogCache_WrongService(t *testing.T) {
	proc := &process.Process{Base: &process.BaseProcess{}}
	proc.Ctx = context.Background()
	_, err := handleGCCatalogCache(proc, tn, "", nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
}

func TestHandleGCCatalogCache_Default(t *testing.T) {
	eng := &mockGCEngine{}
	proc := &process.Process{Base: &process.BaseProcess{}}
	proc.Ctx = context.Background()
	proc.Base.SessionInfo.StorageEngine = eng

	result, err := handleGCCatalogCache(proc, cn, "", nil)
	require.NoError(t, err)
	assert.True(t, eng.called)
	assert.Equal(t, 20*time.Minute, eng.ago)
	assert.Contains(t, result.Data.(string), "20m0s")
}

func TestHandleGCCatalogCache_CustomMinutes(t *testing.T) {
	eng := &mockGCEngine{}
	proc := &process.Process{Base: &process.BaseProcess{}}
	proc.Ctx = context.Background()
	proc.Base.SessionInfo.StorageEngine = eng

	result, err := handleGCCatalogCache(proc, cn, "5", nil)
	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, eng.ago)
	assert.Contains(t, result.Data.(string), "5m0s")
}

func TestHandleGCCatalogCache_MinClamp(t *testing.T) {
	eng := &mockGCEngine{}
	proc := &process.Process{Base: &process.BaseProcess{}}
	proc.Ctx = context.Background()
	proc.Base.SessionInfo.StorageEngine = eng

	_, err := handleGCCatalogCache(proc, cn, "0", nil)
	require.NoError(t, err)
	assert.Equal(t, 1*time.Minute, eng.ago)
}

func TestHandleGCCatalogCache_InvalidParam(t *testing.T) {
	proc := &process.Process{Base: &process.BaseProcess{}}
	proc.Ctx = context.Background()
	_, err := handleGCCatalogCache(proc, cn, "abc", nil)
	require.Error(t, err)
}

func TestHandleGCCatalogCache_EngineNotSupported(t *testing.T) {
	proc := &process.Process{Base: &process.BaseProcess{}}
	proc.Ctx = context.Background()
	proc.Base.SessionInfo.StorageEngine = nil

	result, err := handleGCCatalogCache(proc, cn, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "not supported by engine", result.Data)
}
