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
	"sync/atomic"
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

func TestGlobalSysVarsFencePreventsOldRefreshFromOverwritingNewPublication(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	accountID := ses.GetTenantInfo().GetTenantID()
	globalVars := &SystemVariables{mp: map[string]interface{}{
		PasswordHistory: int64(5),
	}}
	mgr := &GlobalSysVarsMgr{
		accountsGlobalSysVarsMap: map[uint32]*SystemVariables{
			accountID: globalVars,
		},
	}

	oldReadCaptured := make(chan struct{})
	releaseOldRead := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOldRead) }) })
	var reads atomic.Int32
	oldSnapshot := newMrsForGlobalSystemVariables([][]interface{}{{PasswordHistory, "5"}})
	newSnapshot := newMrsForGlobalSystemVariables([][]interface{}{{PasswordHistory, "0"}})
	execStub := gostub.Stub(&ExeSqlInBgSes, func(
		ctx context.Context,
		_ BackgroundExec,
		_ string,
	) ([]ExecResult, error) {
		if reads.Add(1) == 1 {
			close(oldReadCaptured)
			select {
			case <-releaseOldRead:
				return []ExecResult{oldSnapshot}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []ExecResult{newSnapshot}, nil
	})
	t.Cleanup(execStub.Reset)

	type refreshResult struct {
		vars *SystemVariables
		err  error
	}
	oldResult := make(chan refreshResult, 1)
	go func() {
		vars, err := mgr.Get(accountID, ses, context.Background(), nil)
		oldResult <- refreshResult{vars: vars, err: err}
	}()
	select {
	case <-oldReadCaptured:
	case <-time.After(5 * time.Second):
		t.Fatal("old refresh did not capture its catalog snapshot")
	}

	// Linearize the remote SyncCommit ACK, then let a post-fence refresh publish
	// the new catalog value but pause conceptually before its caller clones it.
	mgr.AdvancePublicationEpoch()
	postFenceVars, err := mgr.Get(accountID, ses, context.Background(), nil)
	require.NoError(t, err)
	require.Same(t, globalVars, postFenceVars)
	require.Equal(t, int64(0), postFenceVars.Get(PasswordHistory))

	releaseOnce.Do(func() { close(releaseOldRead) })
	select {
	case result := <-oldResult:
		require.NoError(t, result.err)
		require.Same(t, postFenceVars, result.vars)
	case <-time.After(5 * time.Second):
		t.Fatal("old refresh did not complete")
	}

	// This Clone corresponds to R2 after R1 attempted its delayed publication.
	cloned := postFenceVars.Clone()
	require.Equal(t, int64(0), cloned.Get(PasswordHistory))
}

func TestGlobalSysVarsFencePreventsOldRefreshFromCreatingSharedEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	accountID := ses.GetTenantInfo().GetTenantID()
	mgr := &GlobalSysVarsMgr{accountsGlobalSysVarsMap: make(map[uint32]*SystemVariables)}

	oldReadCaptured := make(chan struct{})
	releaseOldRead := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOldRead) }) })
	oldSnapshot := newMrsForGlobalSystemVariables([][]interface{}{{PasswordHistory, "5"}})
	execStub := gostub.Stub(&ExeSqlInBgSes, func(
		ctx context.Context,
		_ BackgroundExec,
		_ string,
	) ([]ExecResult, error) {
		close(oldReadCaptured)
		select {
		case <-releaseOldRead:
			return []ExecResult{oldSnapshot}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	t.Cleanup(execStub.Reset)

	type refreshResult struct {
		vars *SystemVariables
		err  error
	}
	resultC := make(chan refreshResult, 1)
	go func() {
		vars, err := mgr.Get(accountID, ses, context.Background(), nil)
		resultC <- refreshResult{vars: vars, err: err}
	}()
	select {
	case <-oldReadCaptured:
	case <-time.After(5 * time.Second):
		t.Fatal("old refresh did not capture its catalog snapshot")
	}
	mgr.AdvancePublicationEpoch()
	releaseOnce.Do(func() { close(releaseOldRead) })

	select {
	case result := <-resultC:
		require.NoError(t, result.err)
		require.Equal(t, int64(5), result.vars.Get(PasswordHistory))
	case <-time.After(5 * time.Second):
		t.Fatal("old refresh did not complete")
	}
	mgr.Lock()
	_, published := mgr.accountsGlobalSysVarsMap[accountID]
	mgr.Unlock()
	require.False(t, published, "a pre-fence read must not create a shared cache entry after ACK")
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
