// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestGlobalSysVarsRefreshDoesNotOverwriteConcurrentSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	accountID := ses.GetTenantInfo().GetTenantID()

	globalVars := &SystemVariables{
		mp: map[string]interface{}{
			PasswordHistory: int64(5),
		},
	}
	ses.gSysVars = globalVars
	mgr := &GlobalSysVarsMgr{
		accountsGlobalSysVarsMap: map[uint32]*SystemVariables{
			accountID: globalVars,
		},
	}

	refreshCaptured := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once

	staleSnapshot := newMrsForGlobalSystemVariables([][]interface{}{
		{PasswordHistory, "5"},
	})
	execStub := gostub.Stub(&ExeSqlInBgSes, func(
		ctx context.Context,
		_ BackgroundExec,
		_ string,
	) ([]ExecResult, error) {
		close(refreshCaptured)
		select {
		case <-releaseRefresh:
			return []ExecResult{staleSnapshot}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	t.Cleanup(execStub.Reset)

	stubGlobalSysVarPersistence(t, sysVarSet{PasswordHistory, int64(0)})

	refreshResult := make(chan error, 1)
	refreshDone := make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() {
			close(releaseRefresh)
		})
		select {
		case <-refreshDone:
		case <-time.After(5 * time.Second):
			t.Error("refresh goroutine did not exit")
		}
	})
	go func() {
		defer close(refreshDone)
		_, err := mgr.Get(accountID, ses, context.Background(), nil)
		refreshResult <- err
	}()

	select {
	case <-refreshCaptured:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not capture the stale snapshot")
	}

	require.NoError(t, ses.SetGlobalSysVar(context.Background(), PasswordHistory, int64(0)))
	releaseOnce.Do(func() {
		close(releaseRefresh)
	})

	select {
	case err := <-refreshResult:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not complete")
	}

	value, err := ses.GetGlobalSysVar(PasswordHistory)
	require.NoError(t, err)
	require.Equal(t, int64(0), value)
}

func TestGlobalSysVarsRefreshDoesNotAdvanceMutationGeneration(t *testing.T) {
	globalVars := SystemVariables{
		mp: map[string]interface{}{
			PasswordHistory: int64(5),
		},
	}
	generation := globalVars.getMutationGeneration()

	globalVars.replaceIfMutationGeneration(generation, map[string]interface{}{
		PasswordHistory: int64(5),
	})
	require.Equal(t, generation, globalVars.getMutationGeneration())

	globalVars.replaceIfMutationGeneration(generation, map[string]interface{}{
		PasswordHistory: int64(0),
	})
	value := globalVars.Get(PasswordHistory)
	require.Equal(t, int64(0), value)
}

func TestGlobalSysVarsCatalogEpochRejectsReversePublication(t *testing.T) {
	globalVars := SystemVariables{
		mp: map[string]interface{}{PasswordHistory: int64(0)},
	}
	generation := globalVars.getMutationGeneration()

	globalVars.replaceIfCatalogEpoch(2, generation, map[string]interface{}{
		PasswordHistory: int64(5),
	})
	globalVars.replaceIfCatalogEpoch(1, generation, map[string]interface{}{
		PasswordHistory: int64(0),
	})

	require.Equal(t, uint64(2), globalVars.catalogEpoch)
	require.Equal(t, int64(5), globalVars.Get(PasswordHistory),
		"a late catalog read from an older commit must not roll the cache back")
}

func TestGlobalSysVarsCatalogEpochOrdersConcurrentSetPublication(t *testing.T) {
	globalVars := SystemVariables{
		mp: map[string]interface{}{PasswordHistory: int64(0)},
	}

	globalVars.setAtCatalogEpoch(PasswordHistory, int64(6), 2)
	globalVars.setAtCatalogEpoch(PasswordHistory, int64(5), 1)

	require.Equal(t, uint64(2), globalVars.catalogEpoch)
	require.Equal(t, int64(6), globalVars.Get(PasswordHistory),
		"local completion order must not override catalog commit order")
}
