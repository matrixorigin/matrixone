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

	"github.com/matrixorigin/matrixone/pkg/pb/status"
)

// Session is an interface which should have the following methods.
type Session interface {
	// GetUUIDString returns the id of the session.
	GetUUIDString() string
	// GetTenantName returns the tenant name of the session.
	GetTenantName() string
	// StatusSession converts the session to status.Session.
	StatusSession() *status.Session
	// SetSessionRoutineStatus set the session Status
	SetSessionRoutineStatus(status string) error
}

// SessionManager manages all sessions locally.
type SessionManager struct {
	mu struct {
		sync.RWMutex
		sessionsByID map[string]Session
		// Tenant name => []*Session
		sessionsByTenant map[string]map[Session]struct{}
	}
}

// NewSessionManager creates a new SessionManager instance.
func NewSessionManager() *SessionManager {
	m := &SessionManager{}
	m.mu.sessionsByID = make(map[string]Session)
	m.mu.sessionsByTenant = make(map[string]map[Session]struct{})
	return m
}

// AddSession adds a new session to manager.
func (sm *SessionManager) AddSession(s Session) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.sessionsByID[s.GetUUIDString()] = s
	_, ok := sm.mu.sessionsByTenant[s.GetTenantName()]
	if !ok {
		sm.mu.sessionsByTenant[s.GetTenantName()] = make(map[Session]struct{})
	}
	sm.mu.sessionsByTenant[s.GetTenantName()][s] = struct{}{}
}

// RemoveSession removes a session from manager.
func (sm *SessionManager) RemoveSession(s Session) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.mu.sessionsByID, s.GetUUIDString())
	delete(sm.mu.sessionsByTenant[s.GetTenantName()], s)
}

// GetAllSessions returns all sessions in the manager.
func (sm *SessionManager) GetAllSessions() []Session {
	if sm == nil {
		return nil
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	sessions := make([]Session, 0, len(sm.mu.sessionsByID))
	for _, session := range sm.mu.sessionsByID {
		sessions = append(sessions, session)
	}
	return sessions
}

// GetAllStatusSessions returns all status sessions in the manager.
func (sm *SessionManager) GetAllStatusSessions() []*status.Session {
	if sm == nil {
		return []*status.Session{}
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	sessions := make([]*status.Session, 0, len(sm.mu.sessionsByID))
	for _, session := range sm.mu.sessionsByID {
		sessions = append(sessions, session.StatusSession())
	}
	return sessions
}

// GetSessionsByTenant returns the sessions belongs to the tenant.
func (sm *SessionManager) GetSessionsByTenant(tenant string) []Session {
	if sm == nil {
		return nil
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	sessions := make([]Session, 0, len(sm.mu.sessionsByTenant[tenant]))
	for session := range sm.mu.sessionsByTenant[tenant] {
		sessions = append(sessions, session)
	}
	return sessions
}

// GetStatusSessionsByTenant returns the status sessions belongs to the tenant.
func (sm *SessionManager) GetStatusSessionsByTenant(tenant string) []*status.Session {
	if sm == nil {
		return []*status.Session{}
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	sessions := make([]*status.Session, 0, len(sm.mu.sessionsByTenant[tenant]))
	for session := range sm.mu.sessionsByTenant[tenant] {
		sessions = append(sessions, session.StatusSession())
	}
	return sessions
}
