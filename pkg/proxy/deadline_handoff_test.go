// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// phaseDeadlineConn models the frontend IOSession behavior relevant to the
// proxy handoff: every protocol read arms a relative deadline, while the
// terminal handoff must clear it. It also lets handoff tests exercise the
// transport-error path without sleeping for a real timeout.
type phaseDeadlineConn struct {
	net.Conn
	mu        sync.Mutex
	deadline  time.Time
	failClear bool
}

func (c *phaseDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() && c.failClear {
		return errors.New("read deadline clear failed")
	}
	c.mu.Lock()
	c.deadline = deadline
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(deadline)
}

func (c *phaseDeadlineConn) readDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline
}

// deadlineRearmingServerConn makes each internal control statement look like a
// real frontend IOSession read. The production handoff code must clear the
// deadline after the final statement, not only after the initial handshake.
type deadlineRearmingServerConn struct {
	ServerConn
	raw        *phaseDeadlineConn
	statements *[]string
}

func (s *deadlineRearmingServerConn) ExecStmt(stmt internalStmt, resp chan<- []byte) (bool, error) {
	if s.statements != nil {
		*s.statements = append(*s.statements, stmt.s)
	}
	ok, err := s.ServerConn.ExecStmt(stmt, resp)
	if err != nil {
		return ok, err
	}
	if err := s.raw.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
		return false, err
	}
	return ok, nil
}

func (s *deadlineRearmingServerConn) ExecStmtContext(
	_ context.Context,
	stmt internalStmt,
	resp chan<- []byte,
) (bool, error) {
	return s.ExecStmt(stmt, resp)
}
