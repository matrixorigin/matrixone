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
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetLeaderID(t *testing.T) {
	fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
		rt := runtime.DefaultRuntime()
		lc := newLogClient(common{
			log: rt.Logger(),
		}, 3)
		lc.client = c.(logservice.StandbyClient) // this will skip prepare action.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		leaderID, err := lc.getLeaderID(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), leaderID)
		lc.close()
	}
	logservice.RunClientTest(t, false, nil, fn)
}

func TestWrite(t *testing.T) {
	fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
		rt := runtime.DefaultRuntime()
		lc := newLogClient(common{
			log: rt.Logger(),
		}, 3)
		lc.client = c.(logservice.StandbyClient) // this will skip prepare action.
		rec := c.GetLogRecord(16)
		n, err := rand.Read(rec.Payload())
		assert.NoError(t, err)
		assert.Equal(t, 16, n)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		l, err := lc.writeWithRetry(ctx, rec.Data, 10)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), l)
		lc.close()
	}
	logservice.RunClientTest(t, false, nil, fn)
}

func TestRead(t *testing.T) {
	fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
		rt := runtime.DefaultRuntime()
		lc := newLogClient(common{
			log: rt.Logger(),
		}, 3)
		lc.client = c.(logservice.StandbyClient) // this will skip prepare action.
		rec1 := c.GetLogRecord(16)
		n, err := rand.Read(rec1.Payload())
		assert.NoError(t, err)
		assert.Equal(t, 16, n)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		l, err := lc.write(ctx, rec1.Data)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), l)

		rec2 := c.GetLogRecord(16)
		assert.NoError(t, err)
		assert.Equal(t, 16, n)
		l, err = lc.write(ctx, rec2.Data)
		assert.NoError(t, err)
		assert.Equal(t, uint64(5), l)

		recs, lsn, err := lc.readEntries(ctx, 4)
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)
		require.Equal(t, 2, len(recs))
		assert.Equal(t, rec1.Data, recs[0].Data)
		assert.Equal(t, rec2.Data, recs[1].Data)

		_, _, err = lc.readEntries(ctx, 6)
		assert.True(t, errors.Is(err, dragonboat.ErrInvalidRange))

		lc.close()
	}
	logservice.RunClientTest(t, false, nil, fn)
}

func TestTruncate(t *testing.T) {
	fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
		rt := runtime.DefaultRuntime()
		lc := newLogClient(common{
			log: rt.Logger(),
		}, 3)
		lc.client = c.(logservice.StandbyClient) // this will skip prepare action.
		rec1 := c.GetLogRecord(16)
		n, err := rand.Read(rec1.Payload())
		assert.NoError(t, err)
		assert.Equal(t, 16, n)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		l, err := lc.write(ctx, rec1.Data)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), l)

		require.NoError(t, lc.truncate(ctx, 4))
		lsn, err := lc.getTruncatedLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)

		l, err = lc.write(ctx, rec1.Data)
		assert.NoError(t, err)
		assert.Equal(t, uint64(6), l)

		require.NoError(t, lc.truncate(ctx, 5))
		lsn, err = lc.getTruncatedLsnWithRetry(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(5), lsn)

		err = lc.truncate(ctx, 3)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn))
	}
	logservice.RunClientTest(t, false, nil, fn)
}

func TestGetLatestLsn(t *testing.T) {
	fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
		rt := runtime.DefaultRuntime()
		lc := newLogClient(common{
			log: rt.Logger(),
		}, 3)
		lc.client = c.(logservice.StandbyClient) // this will skip prepare action.
		rec1 := c.GetLogRecord(16)
		n, err := rand.Read(rec1.Payload())
		assert.NoError(t, err)
		assert.Equal(t, 16, n)
		appendFn := func(j int) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			lsn, err := lc.write(ctx, rec1.Data)
			require.NoError(t, err)
			assert.Equal(t, uint64(4+j), lsn)
		}
		for i := 0; i < 10; i++ {
			appendFn(i)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		lsn, err := lc.getLatestLsnWithRetry(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(13), lsn)
	}
	logservice.RunClientTest(t, false, nil, fn)
}

func TestRequiredLsn(t *testing.T) {
	fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
		rt := runtime.DefaultRuntime()
		lc := newLogClient(common{
			log: rt.Logger(),
		}, 3)
		lc.client = c.(logservice.StandbyClient) // this will skip prepare action.
		rec1 := c.GetLogRecord(16)
		n, err := rand.Read(rec1.Payload())
		assert.NoError(t, err)
		assert.Equal(t, 16, n)
		appendFn := func(j int) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			lsn, err := lc.write(ctx, rec1.Data)
			require.NoError(t, err)
			assert.Equal(t, uint64(4+j), lsn)
		}
		for i := 0; i < 10; i++ {
			appendFn(i)
		}

		claimCtx, claimCancel := context.WithCancel(context.Background())
		defer claimCancel()
		require.NoError(t, lc.claimRequiredLsn(claimCtx))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		require.NoError(t, lc.setRequiredLsn(ctx, 8))
		lsn, err := lc.getRequiredLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(8), lsn)

		require.NoError(t, lc.setRequiredLsnWithRetry(ctx, 9))
		lsn, err = lc.getRequiredLsnWithRetry(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(9), lsn)

		err = lc.setRequiredLsn(ctx, 100)
		assert.NoError(t, err)
		lsn, err = lc.getRequiredLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), lsn)

		require.NoError(t, lc.resetRequiredLsn(ctx))
		lsn, err = lc.getRequiredLsn(ctx)
		require.NoError(t, err)
		require.Zero(t, lsn)
	}
	logservice.RunClientTest(t, false, nil, fn)
}

// requiredStateClient models the replicated ownership state while allowing a
// committed proposal's response to be lost before DataSync sees it.
type requiredStateClient struct {
	logservice.StandbyClient
	lsn, revision, owner uint64
	leaderID, term       uint64
	loseClaimResponse    bool
	loseSetResponse      bool
	rejectClaim          bool
	failReadback         bool
	takeoverAfterSet     bool
	changeResetReadback  bool
	stateReads           int
}

func (c *requiredStateClient) GetLeaderState(context.Context) (uint64, uint64, error) {
	return c.leaderID, c.term, nil
}

func (c *requiredStateClient) GetRequiredLsnState(context.Context) (logservice.Lsn, uint64, uint64, error) {
	c.stateReads++
	if c.failReadback && c.stateReads > 1 {
		return 0, 0, 0, fakeError
	}
	if c.changeResetReadback && c.lsn == 0 && c.stateReads > 1 {
		// Another owner claimed the shard between the reset and its readback.
		c.owner++
		c.revision++
	}
	return c.lsn, c.revision, c.owner, nil
}

func (c *requiredStateClient) ClaimRequiredLsn(
	_ context.Context, expected logservice.Lsn, revision, owner, term, leaderID uint64,
) error {
	if c.rejectClaim {
		return fakeError
	}
	if c.lsn != expected || c.revision != revision ||
		c.term != term || c.leaderID != leaderID {
		return errRequiredLsnConflict
	}
	c.owner = owner
	c.revision++
	if c.loseClaimResponse {
		return fakeError
	}
	return nil
}

func (c *requiredStateClient) SetRequiredLsnIfCurrent(
	_ context.Context, lsn, expected logservice.Lsn, revision, owner uint64,
) error {
	if c.lsn != expected || c.revision != revision || c.owner != owner {
		return errRequiredLsnConflict
	}
	c.lsn = lsn
	c.revision++
	if c.takeoverAfterSet {
		c.owner++
		c.revision++
		return fakeError
	}
	if c.loseSetResponse {
		return fakeError
	}
	return nil
}

func TestRequiredLsnOwnershipAfterLostResponses(t *testing.T) {
	for _, tc := range []struct {
		name                string
		loseClaimResponse   bool
		loseSetResponse     bool
		takeoverAfterSet    bool
		changeResetReadback bool
	}{
		{name: "claim response lost", loseClaimResponse: true},
		{name: "set response lost", loseSetResponse: true},
		{name: "new owner after set", takeoverAfterSet: true},
		{name: "new owner before reset readback", changeResetReadback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &requiredStateClient{
				leaderID: 2, term: 1,
				loseClaimResponse:   tc.loseClaimResponse,
				loseSetResponse:     tc.loseSetResponse,
				takeoverAfterSet:    tc.takeoverAfterSet,
				changeResetReadback: tc.changeResetReadback,
			}
			lc := &logClient{client: server}
			ctx := context.Background()
			require.NoError(t, lc.claimRequiredLsn(ctx))
			require.NotZero(t, lc.requiredOwner)
			if tc.takeoverAfterSet {
				require.ErrorIs(t, lc.setRequiredLsn(ctx, 100), errRequiredLsnConflict)
				require.NotEqual(t, lc.requiredRevision, server.revision)
				return
			}
			require.NoError(t, lc.setRequiredLsn(ctx, 100))
			if tc.changeResetReadback {
				require.ErrorIs(t, lc.resetRequiredLsn(ctx), errRequiredLsnConflict)
				require.NotEqual(t, lc.requiredOwner, server.owner)
				return
			}
			require.NoError(t, lc.resetRequiredLsn(ctx))
			require.Zero(t, server.lsn)
		})
	}
}

func TestRequiredLsnClaimRejectsStaleLeadership(t *testing.T) {
	ctx := context.Background()
	t.Run("no leader", func(t *testing.T) {
		server := &requiredStateClient{term: 1}
		lc := &logClient{client: server}
		require.ErrorIs(t, lc.claimRequiredLsn(ctx), errRequiredLsnConflict)
		require.Zero(t, server.revision)
	})
	t.Run("local replica is no longer leader", func(t *testing.T) {
		server := &requiredStateClient{leaderID: 2, term: 1}
		localReplicas := &sync.Map{}
		localReplicas.Store(uint64(3), uint64(1))
		lc := &logClient{common: common{shardReplicaID: localReplicas}, client: server, shardID: 3}
		require.ErrorIs(t, lc.claimRequiredLsn(ctx), errRequiredLsnConflict)
		require.Zero(t, server.revision)
	})
	t.Run("failed proposal did not commit", func(t *testing.T) {
		server := &requiredStateClient{leaderID: 2, term: 1, rejectClaim: true}
		lc := &logClient{client: server}
		require.ErrorIs(t, lc.claimRequiredLsn(ctx), errRequiredLsnConflict)
		require.Zero(t, lc.requiredOwner)
		require.Zero(t, server.revision)
	})
}

func TestRequiredLsnSetNeedsVerifiableCommit(t *testing.T) {
	server := &requiredStateClient{leaderID: 2, term: 1, loseSetResponse: true}
	lc := &logClient{client: server}
	ctx := context.Background()
	require.NoError(t, lc.claimRequiredLsn(ctx))
	server.failReadback = true
	require.ErrorIs(t, lc.setRequiredLsn(ctx, 100), fakeError)
	require.Zero(t, lc.requiredLsn)
	require.Equal(t, uint64(100), server.lsn)
}

type serverValue struct {
	shardID          uint64
	truncatedLsn     uint64
	latestLsn        uint64
	requiredLsn      uint64
	requiredRevision uint64
	requiredOwner    uint64
	entries          []logservice.LogRecord
	// fake error
	fakeErr      map[string]struct{}
	fakeErrCount map[string]int
	callCount    map[string]int
	failureCount map[string]int
}

type mockLogServer struct {
	mu sync.Mutex
	// shardID => *serverValue
	values map[uint64]*serverValue
}

func newMockServer() *mockLogServer {
	return &mockLogServer{
		values: make(map[uint64]*serverValue),
	}
}

type mockLogClient struct {
	shardID              uint64
	s                    *mockLogServer
	getLeaderIDEvent     chan<- uint64
	readEntriesEvent     chan<- struct{}
	getTruncatedLsnEvent chan<- struct{}
	requiredRevision     uint64
	requiredOwner        uint64
}

func newMockLogClient(s *mockLogServer, shardID uint64) LogClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.values[shardID]
	if !ok || s.values[shardID] == nil {
		s.values[shardID] = &serverValue{
			fakeErr:      make(map[string]struct{}),
			fakeErrCount: make(map[string]int),
			callCount:    make(map[string]int),
			failureCount: make(map[string]int),
		}
	}
	return &mockLogClient{
		shardID: shardID,
		s:       s,
	}
}

func (m *mockLogClient) getShardID() uint64 {
	return m.shardID
}

var (
	fakeError = moerr.NewInternalErrorNoCtx("fake error")
)

func signalTestEvent(ch chan<- struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (m *mockLogClient) getLeaderID(ctx context.Context) (uint64, error) {
	m.s.mu.Lock()
	leader := m.s.values[m.shardID].shardID
	_, failed := m.s.values[m.shardID].fakeErr["getLeaderID"]
	m.s.mu.Unlock()
	if failed {
		return 0, fakeError
	}
	// Publish the captured return value without holding the server mutex.
	if m.getLeaderIDEvent != nil {
		select {
		case m.getLeaderIDEvent <- leader:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return leader, nil
}

func (m *mockLogClient) write(_ context.Context, data []byte) (uint64, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if _, ok := m.s.values[m.shardID].fakeErr["write"]; ok {
		return 0, fakeError
	}
	m.s.values[m.shardID].latestLsn += 1
	m.s.values[m.shardID].entries = append(
		m.s.values[m.shardID].entries,
		logservice.LogRecord{
			Lsn:  m.s.values[m.shardID].latestLsn,
			Data: data,
		},
	)
	return m.s.values[m.shardID].latestLsn, nil
}

func (m *mockLogClient) readEntries(_ context.Context, lsn uint64) ([]logservice.LogRecord, uint64, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	defer signalTestEvent(m.readEntriesEvent)
	if _, ok := m.s.values[m.shardID].fakeErr["readEntries"]; ok {
		return nil, 0, fakeError
	}
	var index int
	for i, e := range m.s.values[m.shardID].entries {
		if e.Lsn == lsn {
			index = i
			break
		}
	}
	return m.s.values[m.shardID].entries[index:], lsn, nil
}

func (m *mockLogClient) truncate(_ context.Context, lsn uint64) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if _, ok := m.s.values[m.shardID].fakeErr["truncate"]; ok {
		return fakeError
	}
	m.s.values[m.shardID].truncatedLsn = lsn
	return nil
}

func (m *mockLogClient) getTruncatedLsn(_ context.Context) (uint64, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	defer signalTestEvent(m.getTruncatedLsnEvent)
	if _, ok := m.s.values[m.shardID].fakeErr["getTruncatedLsn"]; ok {
		return 0, fakeError
	}
	return m.s.values[m.shardID].truncatedLsn, nil
}

func (m *mockLogClient) getLatestLsn(_ context.Context) (uint64, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if m.shouldFakeErrorLocked("getLatestLsn") {
		return 0, fakeError
	}
	return m.s.values[m.shardID].latestLsn, nil
}

func (m *mockLogClient) setRequiredLsn(_ context.Context, lsn uint64) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if m.shouldFakeErrorLocked("setRequiredLsn") {
		return fakeError
	}
	state := m.s.values[m.shardID]
	if m.requiredOwner == 0 {
		state.requiredOwner++
		state.requiredRevision++
		m.requiredOwner = state.requiredOwner
		m.requiredRevision = state.requiredRevision
	}
	if state.requiredOwner != m.requiredOwner || state.requiredRevision != m.requiredRevision ||
		(lsn != 0 && lsn < state.requiredLsn) {
		return errRequiredLsnConflict
	}
	state.requiredLsn = lsn
	state.requiredRevision++
	m.requiredRevision++
	return nil
}

func (m *mockLogClient) claimRequiredLsn(_ context.Context) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	state := m.s.values[m.shardID]
	state.requiredOwner++
	state.requiredRevision++
	m.requiredOwner = state.requiredOwner
	m.requiredRevision = state.requiredRevision
	return nil
}

func (m *mockLogClient) resetRequiredLsn(ctx context.Context) error {
	if err := m.setRequiredLsn(ctx, 0); err != nil {
		return err
	}
	lsn, err := m.getRequiredLsn(ctx)
	if err != nil {
		return err
	}
	if lsn != 0 {
		return errRequiredLsnConflict
	}
	return nil
}

func (m *mockLogClient) getRequiredLsn(_ context.Context) (uint64, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if _, ok := m.s.values[m.shardID].fakeErr["getRequiredLsn"]; ok {
		return 0, fakeError
	}
	return m.s.values[m.shardID].requiredLsn, nil
}

func (m *mockLogClient) writeWithRetry(ctx context.Context, data []byte, _ int) (uint64, error) {
	return m.write(ctx, data)
}

func (m *mockLogClient) getTruncatedLsnWithRetry(ctx context.Context) (uint64, error) {
	return m.getTruncatedLsn(ctx)
}

func (m *mockLogClient) setRequiredLsnWithRetry(ctx context.Context, lsn uint64) error {
	return m.setRequiredLsn(ctx, lsn)
}

func (m *mockLogClient) getRequiredLsnWithRetry(ctx context.Context) (uint64, error) {
	return m.getRequiredLsn(ctx)
}

func (m *mockLogClient) getLatestLsnWithRetry(ctx context.Context) (uint64, error) {
	return m.getLatestLsn(ctx)
}

func (m *mockLogClient) close() {}

func (m *mockLogClient) fakeError(tag string) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	m.s.values[m.shardID].fakeErr[tag] = struct{}{}
}

func (m *mockLogClient) clearFakeError(tag string) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	delete(m.s.values[m.shardID].fakeErr, tag)
}

func (m *mockLogClient) failNext(tag string, count int) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	v := m.s.values[m.shardID]
	v.fakeErrCount[tag] = count
	v.callCount[tag] = 0
	v.failureCount[tag] = 0
}

func (m *mockLogClient) callStats(tag string) (calls, failures int) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	v := m.s.values[m.shardID]
	return v.callCount[tag], v.failureCount[tag]
}

func (m *mockLogClient) shouldFakeErrorLocked(tag string) bool {
	v := m.s.values[m.shardID]
	v.callCount[tag]++
	if v.fakeErrCount[tag] > 0 {
		v.fakeErrCount[tag]--
		v.failureCount[tag]++
		return true
	}
	if _, ok := v.fakeErr[tag]; ok {
		v.failureCount[tag]++
		return true
	}
	return false
}

func (m *mockLogClient) setLeaderID(id uint64) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	m.s.values[m.shardID].shardID = id
}

// retryStandbyClient exercises production logClient retries without a cluster.
type retryStandbyClient struct {
	logservice.StandbyClient
	call   func(context.Context) (uint64, error)
	setLSN uint64
}

func (c *retryStandbyClient) Append(ctx context.Context, _ pb.LogRecord) (logservice.Lsn, error) {
	return c.call(ctx)
}

func (c *retryStandbyClient) GetTruncatedLsn(ctx context.Context) (logservice.Lsn, error) {
	return c.call(ctx)
}

func (c *retryStandbyClient) GetLatestLsn(ctx context.Context) (logservice.Lsn, error) {
	return c.call(ctx)
}

func (c *retryStandbyClient) GetRequiredLsn(ctx context.Context) (logservice.Lsn, error) {
	return c.call(ctx)
}

func (c *retryStandbyClient) SetRequiredLsn(ctx context.Context, lsn logservice.Lsn) error {
	c.setLSN = lsn
	_, err := c.call(ctx)
	return err
}

func (c *retryStandbyClient) SetRequiredLsnIfCurrent(ctx context.Context, lsn, expected logservice.Lsn, revision, owner uint64) error {
	return c.SetRequiredLsn(ctx, lsn)
}

func (c *retryStandbyClient) ClaimRequiredLsn(ctx context.Context, expected logservice.Lsn, revision, owner, term, leaderID uint64) error {
	_, err := c.call(ctx)
	return err
}

func (c *retryStandbyClient) GetRequiredLsnState(ctx context.Context) (logservice.Lsn, uint64, uint64, error) {
	return 0, 0, 0, fakeError
}

func (c *retryStandbyClient) GetLeaderState(context.Context) (uint64, uint64, error) {
	return 2, 1, nil
}

func (c *retryStandbyClient) TruncateWAL(ctx context.Context, psn, dsn logservice.Lsn) error {
	return c.Truncate(ctx, psn)
}

func (c *retryStandbyClient) GetWALTruncate(ctx context.Context) (logservice.Lsn, logservice.Lsn, error) {
	psn, err := c.GetTruncatedLsn(ctx)
	return psn, 0, err
}

func (c *retryStandbyClient) Close() error { return nil }

func TestLogClientRetryContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*logClient, context.Context) (uint64, error)
		want uint64
	}{
		{"write", func(c *logClient, ctx context.Context) (uint64, error) {
			return c.writeWithRetry(ctx, nil, 2)
		}, 41},
		{"get truncated", (*logClient).getTruncatedLsnWithRetry, 41},
		{"get latest", (*logClient).getLatestLsnWithRetry, 41},
		{"get required", (*logClient).getRequiredLsnWithRetry, 41},
		{"set required", func(c *logClient, ctx context.Context) (uint64, error) {
			return 0, c.setRequiredLsnWithRetry(ctx, 99)
		}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"precanceled", "cancel on error", "retry succeeds"} {
				t.Run(mode, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					calls := 0
					c := &logClient{requiredOwner: 1, client: &retryStandbyClient{call: func(context.Context) (uint64, error) {
						calls++
						if calls == 1 && mode != "precanceled" {
							if mode == "cancel on error" {
								cancel()
							}
							return 0, fakeError
						}
						return 41, nil
					}}}
					defer c.close()
					if mode == "precanceled" {
						cancel()
					}
					lsn, err := tc.call(c, ctx)
					if tc.name == "set required" && calls > 0 {
						require.Equal(t, uint64(99), c.client.(*retryStandbyClient).setLSN)
					}
					if mode == "retry succeeds" {
						require.NoError(t, err)
						require.NoError(t, ctx.Err())
						require.Equal(t, tc.want, lsn)
						require.Equal(t, 2, calls)
						return
					}
					require.ErrorIs(t, err, context.Canceled)
					require.Zero(t, lsn)
					wantCalls := 1
					if mode == "precanceled" {
						wantCalls = 0
					}
					require.Equal(t, wantCalls, calls)
				})
			}
		})
	}

	t.Run("write retry limit", func(t *testing.T) {
		calls := 0
		c := &logClient{client: &retryStandbyClient{call: func(context.Context) (uint64, error) {
			calls++
			return 0, fakeError
		}}}
		defer c.close()
		_, err := c.writeWithRetry(context.Background(), nil, 1)
		require.ErrorIs(t, err, fakeError)
		require.Equal(t, 1, calls)
	})
}
