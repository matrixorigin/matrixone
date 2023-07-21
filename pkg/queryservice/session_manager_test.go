// Copyright 2021 - 2023 Matrix Origin
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

package queryservice

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/stretchr/testify/assert"
)

type mockSession struct {
	id     string
	tenant string
}

func (s *mockSession) GetUUIDString() string {
	return s.id
}

func (s *mockSession) GetTenantName() string {
	return s.tenant
}

func (s *mockSession) StatusSession() *status.Session {
	return &status.Session{
		SessionID: s.id,
		Account:   s.tenant,
	}
}

func TestNewSessionManager(t *testing.T) {
	sm := NewSessionManager()
	assert.NotNil(t, sm)
	assert.NotNil(t, sm.mu.sessionsByID)
	assert.NotNil(t, sm.mu.sessionsByTenant)
}

func TestSessionManagerMain(t *testing.T) {
	sm := NewSessionManager()
	assert.NotNil(t, sm)
	s1 := &mockSession{
		id:     uuid.NewString(),
		tenant: "t1",
	}
	sm.AddSession(s1)
	ss := sm.GetAllSessions()
	assert.NotNil(t, ss)
	assert.Equal(t, 1, len(ss))
	assert.Equal(t, s1, ss[0])

	ss = sm.GetSessionsByTenant("t1")
	assert.NotNil(t, ss)
	assert.Equal(t, 1, len(ss))
	assert.Equal(t, s1, ss[0])

	ss = sm.GetSessionsByTenant("t2")
	assert.NotNil(t, ss)
	assert.Equal(t, 0, len(ss))

	sm.RemoveSession(s1)
	ss = sm.GetAllSessions()
	assert.NotNil(t, ss)
	assert.Equal(t, 0, len(ss))
}

func TestSessionManagerParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sm := NewSessionManager()
	assert.NotNil(t, sm)
	var wg sync.WaitGroup
	count := 100
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(t *testing.T) {
			sm.AddSession(&mockSession{
				id: uuid.NewString(),
			})
			wg.Done()
		}(t)
	}
	wg.Wait()
	ss := sm.GetAllSessions()
	assert.NotNil(t, ss)
	assert.Equal(t, count, len(ss))
}
