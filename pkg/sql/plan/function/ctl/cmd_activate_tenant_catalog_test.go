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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type fakeActivatorEngine struct {
	engine.Engine
	called    bool
	accountID uint32
	err       error
}

func (f *fakeActivatorEngine) ActivateTenantCatalog(_ context.Context, accountID uint32) error {
	f.called = true
	f.accountID = accountID
	return f.err
}

func makeActivateProc(ctx context.Context, eng engine.Engine) *process.Process {
	proc := process.NewTopProcess(ctx, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	proc.Base.SessionInfo.StorageEngine = eng
	return proc
}

func TestHandleActivateTenantCatalog_NotCN(t *testing.T) {
	proc := makeActivateProc(context.Background(), nil)
	_, err := handleActivateTenantCatalog(proc, tn, "1", nil)
	require.Error(t, err)
}

func TestHandleActivateTenantCatalog_NoAccountInCtx(t *testing.T) {
	proc := makeActivateProc(context.Background(), nil)
	_, err := handleActivateTenantCatalog(proc, cn, "1", nil)
	require.Error(t, err)
}

func TestHandleActivateTenantCatalog_NotSysAccount(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 42)
	proc := makeActivateProc(ctx, nil)
	_, err := handleActivateTenantCatalog(proc, cn, "1", nil)
	require.Error(t, err)
}

func TestHandleActivateTenantCatalog_InvalidParameter(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 0)
	proc := makeActivateProc(ctx, nil)
	_, err := handleActivateTenantCatalog(proc, cn, "not-a-number", nil)
	require.Error(t, err)
}

type plainEngine struct{ engine.Engine }

func TestHandleActivateTenantCatalog_EngineDoesNotSupport(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 0)
	proc := makeActivateProc(ctx, &plainEngine{})
	res, err := handleActivateTenantCatalog(proc, cn, "5", nil)
	require.NoError(t, err)
	require.Equal(t, ActivateTenantCatalogMethod, res.Method)
}

func TestHandleActivateTenantCatalog_Success(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 0)
	eng := &fakeActivatorEngine{}
	proc := makeActivateProc(ctx, eng)
	res, err := handleActivateTenantCatalog(proc, cn, " 7 ", nil)
	require.NoError(t, err)
	require.Equal(t, ActivateTenantCatalogMethod, res.Method)
	require.True(t, eng.called)
	require.Equal(t, uint32(7), eng.accountID)
}

func TestHandleActivateTenantCatalog_ActivatorError(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 0)
	eng := &fakeActivatorEngine{err: context.Canceled}
	proc := makeActivateProc(ctx, eng)
	_, err := handleActivateTenantCatalog(proc, cn, "9", nil)
	require.Error(t, err)
}
