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
		lsn, err = lc.getRequiredLsn(ctx) // return the max index
		assert.NoError(t, err)
		assert.Equal(t, uint64(16), lsn)
	}
	logservice.RunClientTest(t, false, nil, fn)
}

type serverValue struct {
	shardID      uint64
	truncatedLsn uint64
	latestLsn    uint64
	requiredLsn  uint64
	entries      []logservice.LogRecord
	// fake error
	fakeErr map[string]struct{}
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
	shardID uint64
	s       *mockLogServer
}

func newMockLogClient(s *mockLogServer, shardID uint64) LogClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.values[shardID]
	if !ok || s.values[shardID] == nil {
		s.values[shardID] = &serverValue{
			fakeErr: make(map[string]struct{}),
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

func (m *mockLogClient) getLeaderID(_ context.Context) (uint64, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if _, ok := m.s.values[m.shardID].fakeErr["getLeaderID"]; ok {
		return 0, fakeError
	}
	return m.s.values[m.shardID].shardID, nil
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
	if _, ok := m.s.values[m.shardID].fakeErr["getTruncatedLsn"]; ok {
		return 0, fakeError
	}
	return m.s.values[m.shardID].truncatedLsn, nil
}

func (m *mockLogClient) getLatestLsn(_ context.Context) (uint64, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if _, ok := m.s.values[m.shardID].fakeErr["getLatestLsn"]; ok {
		return 0, fakeError
	}
	return m.s.values[m.shardID].latestLsn, nil
}

func (m *mockLogClient) setRequiredLsn(_ context.Context, lsn uint64) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if _, ok := m.s.values[m.shardID].fakeErr["setRequiredLsn"]; ok {
		return fakeError
	}
	m.s.values[m.shardID].requiredLsn = lsn
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

func (m *mockLogClient) setLeaderID(id uint64) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	m.s.values[m.shardID].shardID = id
}
