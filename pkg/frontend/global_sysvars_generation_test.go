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

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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
	mgr.AdvancePublicationEpoch(timestamp.Timestamp{PhysicalTime: 100})
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

func TestGlobalSysVarsFenceRetriesOldRefreshBeforeCreatingSharedEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	accountID := ses.GetTenantInfo().GetTenantID()
	mgr := &GlobalSysVarsMgr{accountsGlobalSysVarsMap: make(map[uint32]*SystemVariables)}

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
	mgr.AdvancePublicationEpoch(timestamp.Timestamp{PhysicalTime: 100})
	releaseOnce.Do(func() { close(releaseOldRead) })

	select {
	case result := <-resultC:
		require.NoError(t, result.err)
		require.Equal(t, int64(0), result.vars.Get(PasswordHistory))
	case <-time.After(5 * time.Second):
		t.Fatal("old refresh did not complete")
	}
	mgr.Lock()
	published := mgr.accountsGlobalSysVarsMap[accountID]
	mgr.Unlock()
	require.NotNil(t, published)
	require.Equal(t, int64(0), published.Get(PasswordHistory))
	require.Equal(t, int32(2), reads.Load())
}

func TestGlobalSysVarsLaterFenceCannotReturnOlderSharedCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	accountID := ses.GetTenantInfo().GetTenantID()
	globalVars := &SystemVariables{mp: map[string]interface{}{
		PasswordHistory: int64(5),
	}}
	mgr := &GlobalSysVarsMgr{
		accountsGlobalSysVarsMap: map[uint32]*SystemVariables{accountID: globalVars},
	}

	// E1 represents the completed SET GLOBAL whose value the new session must see.
	mgr.AdvancePublicationEpoch(timestamp.Timestamp{PhysicalTime: 100})
	firstReadCaptured := make(chan struct{})
	releaseFirstRead := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirstRead) }) })
	var reads atomic.Int32
	newSnapshot := newMrsForGlobalSystemVariables([][]interface{}{{PasswordHistory, "0"}})
	execStub := gostub.Stub(&ExeSqlInBgSes, func(
		ctx context.Context,
		_ BackgroundExec,
		_ string,
	) ([]ExecResult, error) {
		if reads.Add(1) == 1 {
			close(firstReadCaptured)
			select {
			case <-releaseFirstRead:
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
	resultC := make(chan refreshResult, 1)
	go func() {
		vars, err := mgr.Get(accountID, ses, context.Background(), nil)
		resultC <- refreshResult{vars: vars, err: err}
	}()
	select {
	case <-firstReadCaptured:
	case <-time.After(5 * time.Second):
		t.Fatal("post-E1 refresh did not capture the new catalog value")
	}

	// An unrelated SyncCommit advances the CN-wide epoch to E2 while the E1
	// refresh is waiting to publish. The old shared cache still contains 5.
	mgr.AdvancePublicationEpoch(timestamp.Timestamp{PhysicalTime: 200})
	releaseOnce.Do(func() { close(releaseFirstRead) })

	select {
	case result := <-resultC:
		require.NoError(t, result.err)
		require.Equal(t, int64(0), result.vars.Clone().Get(PasswordHistory))
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not retry after the later fence")
	}
	require.Equal(t, int64(0), globalVars.Get(PasswordHistory))
	require.Equal(t, int32(2), reads.Load())
	require.False(t, globalVars.SetIfNewerCommitTS(
		PasswordHistory, int64(5), timestamp.Timestamp{PhysicalTime: 150}),
		"a delayed local publication must not overwrite a catalog snapshot fenced at E2")
	require.Equal(t, int64(0), globalVars.Get(PasswordHistory))
}

func TestGlobalSysVarsCrossCNNewerFenceForcesDelayedSetterRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	accountID := ses.GetTenantInfo().GetTenantID()
	cn1Vars := &SystemVariables{mp: map[string]interface{}{PasswordHistory: int64(5)}}
	cn2Vars := &SystemVariables{mp: map[string]interface{}{PasswordHistory: int64(5)}}
	cn1Mgr := &GlobalSysVarsMgr{
		accountsGlobalSysVarsMap: map[uint32]*SystemVariables{accountID: cn1Vars},
	}
	cn2Mgr := &GlobalSysVarsMgr{
		accountsGlobalSysVarsMap: map[uint32]*SystemVariables{accountID: cn2Vars},
	}
	ses.gSysVars = cn1Vars

	commitTS1 := timestamp.Timestamp{PhysicalTime: 100}
	commitTS2 := timestamp.Timestamp{PhysicalTime: 200}
	newSnapshot := newMrsForGlobalSystemVariables([][]interface{}{{PasswordHistory, "1"}})
	var reads atomic.Int32
	execStub := gostub.Stub(&ExeSqlInBgSes, func(
		context.Context,
		BackgroundExec,
		string,
	) ([]ExecResult, error) {
		reads.Add(1)
		return []ExecResult{newSnapshot}, nil
	})
	t.Cleanup(execStub.Reset)

	releaseTS1Fence := make(chan struct{})
	ts1Done := make(chan error, 1)
	go func() {
		<-releaseTS1Fence
		ts1Done <- cn1Mgr.PublishCommittedGlobalSysVar(
			context.Background(), ses, PasswordHistory, int64(0), commitTS1)
	}()

	// CN2 publishes TS2 in its independent cache, then its SyncCommit reaches
	// CN1 before CN1's older TS1 fence returns.
	require.NoError(t, cn2Mgr.PublishCommittedGlobalSysVar(
		context.Background(), &Session{feSessionImpl: feSessionImpl{
			tenant:   ses.GetTenantInfo(),
			gSysVars: cn2Vars,
		}}, PasswordHistory, int64(1), commitTS2))
	cn1Mgr.AdvancePublicationEpoch(commitTS2)
	close(releaseTS1Fence)

	select {
	case err := <-ts1Done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("delayed TS1 setter did not complete")
	}
	require.Equal(t, int64(1), cn2Vars.Get(PasswordHistory))
	require.Equal(t, int64(1), cn1Vars.Get(PasswordHistory))
	require.Same(t, cn1Vars, ses.gSysVars)
	require.Equal(t, int32(1), reads.Load(), "CN1 must reload catalog after observing TS2")
}

func TestGlobalSysVarsCrossCNRefreshFailureRejectsDelayedPublication(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	accountID := ses.GetTenantInfo().GetTenantID()
	cn1Vars := &SystemVariables{mp: map[string]interface{}{PasswordHistory: int64(5)}}
	cn1Mgr := &GlobalSysVarsMgr{
		accountsGlobalSysVarsMap: map[uint32]*SystemVariables{accountID: cn1Vars},
	}
	ses.gSysVars = cn1Vars
	cn1Mgr.AdvancePublicationEpoch(timestamp.Timestamp{PhysicalTime: 200})
	execStub := gostub.Stub(&ExeSqlInBgSes, func(
		context.Context,
		BackgroundExec,
		string,
	) ([]ExecResult, error) {
		return nil, context.Canceled
	})
	t.Cleanup(execStub.Reset)

	err := cn1Mgr.PublishCommittedGlobalSysVar(
		context.Background(), ses, PasswordHistory, int64(0), timestamp.Timestamp{PhysicalTime: 100})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(5), cn1Vars.Get(PasswordHistory),
		"failed authoritative refresh must not publish delayed TS1")
}

func TestGlobalSysVarsConcurrentSetPublishesInCommitOrder(t *testing.T) {
	globalVars := &SystemVariables{mp: map[string]interface{}{
		PasswordHistory: int64(5),
	}}
	commitTS1 := timestamp.Timestamp{PhysicalTime: 100}
	commitTS2 := timestamp.Timestamp{PhysicalTime: 200}
	releaseTS1Fence := make(chan struct{})
	ts1Done := make(chan bool, 1)

	// SET A has already committed value 0 at TS1, but its fence returns later.
	go func() {
		<-releaseTS1Fence
		ts1Done <- globalVars.SetIfNewerCommitTS(PasswordHistory, int64(0), commitTS1)
	}()

	// SET B commits later at TS2 and completes its fence first.
	require.True(t, globalVars.SetIfNewerCommitTS(PasswordHistory, int64(1), commitTS2))
	close(releaseTS1Fence)
	select {
	case published := <-ts1Done:
		require.False(t, published, "TS1 must not overwrite the publication from TS2")
	case <-time.After(5 * time.Second):
		t.Fatal("TS1 setter did not complete")
	}
	require.Equal(t, int64(1), globalVars.Get(PasswordHistory))
}

func TestGlobalSysVarsCommitOrderIsTrackedPerVariable(t *testing.T) {
	globalVars := &SystemVariables{mp: map[string]interface{}{
		PasswordHistory:       int64(0),
		PasswordReuseInterval: int64(0),
	}}
	laterTS := timestamp.Timestamp{PhysicalTime: 200}
	earlierTS := timestamp.Timestamp{PhysicalTime: 100}

	require.True(t, globalVars.SetIfNewerCommitTS(PasswordHistory, int64(1), laterTS))
	require.True(t, globalVars.SetIfNewerCommitTS(PasswordReuseInterval, int64(2), earlierTS),
		"a later publication for another variable must not reject this commit")
	require.Equal(t, int64(1), globalVars.Get(PasswordHistory))
	require.Equal(t, int64(2), globalVars.Get(PasswordReuseInterval))
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
	}, timestamp.Timestamp{})
	require.Equal(t, generation, globalVars.getMutationGeneration())

	globalVars.replaceIfMutationGeneration(generation, map[string]interface{}{
		PasswordHistory: int64(0),
	}, timestamp.Timestamp{})
	value := globalVars.Get(PasswordHistory)
	require.Equal(t, int64(0), value)
}
