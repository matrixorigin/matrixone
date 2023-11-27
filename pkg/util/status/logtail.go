// Copyright 2021 -2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package status

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
)

type SessionStatus struct {
	ClientAddress    string    `json:"client_address"`
	LastBeforeSend   time.Time `json:"last_before_send"`
	LastAfterSend    time.Time `json:"last_after_send"`
	Active           int       `json:"active"`
	TableStatusCount int       `json:"table_status_count"`
}

type DeletedSession struct {
	Address   string    `json:"address"`
	DeletedAt time.Time `json:"deleted_at"`
}

type LogtailServerStatus struct {
	Sessions        []SessionStatus  `json:"session_status"`
	DeletedSessions []DeletedSession `json:"deleted_sessions"`
}

func (s *Status) fillLogtail(logtailServer *service.LogtailServer) {
	sessions := logtailServer.SessionMgr().ListSession()
	s.LogtailServerStatus.Sessions = make([]SessionStatus, 0, len(sessions))
	for _, session := range sessions {
		s.LogtailServerStatus.Sessions = append(s.LogtailServerStatus.Sessions, SessionStatus{
			ClientAddress:    session.RemoteAddress(),
			LastBeforeSend:   session.LastBeforeSend(),
			LastAfterSend:    session.LastAfterSend(),
			Active:           session.Active(),
			TableStatusCount: len(session.Tables()),
		})
	}

	deleted := logtailServer.SessionMgr().DeletedSessions()
	s.LogtailServerStatus.DeletedSessions = make([]DeletedSession, 0, len(deleted))
	for _, session := range deleted {
		s.LogtailServerStatus.DeletedSessions = append(s.LogtailServerStatus.DeletedSessions, DeletedSession{
			Address:   session.RemoteAddress(),
			DeletedAt: session.DeletedAt(),
		})
	}
}
