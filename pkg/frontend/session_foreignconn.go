// Copyright 2026 Matrix Origin
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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Session implements process.ForeignConnCache so that esql_tvf / sql_tvf and
// their connect/disconnect builtins can cache foreign-data-source connections
// for the lifetime of the interactive session. All connections are closed by
// closeForeignConns when the session ends. Handles are derived from the
// connection config, so reconnecting with the same config reuses the entry.
var _ process.ForeignConnCache = (*Session)(nil)

// PutForeignConn stores conn under handle. If a connection already exists for
// the handle it is closed first, so a re-registered handle never leaks.
func (ses *Session) PutForeignConn(handle string, conn process.ForeignConn) {
	ses.foreignConnMu.Lock()
	if ses.foreignConns == nil {
		ses.foreignConns = make(map[string]process.ForeignConn)
	}
	old, exists := ses.foreignConns[handle]
	ses.foreignConns[handle] = conn
	ses.foreignConnMu.Unlock()

	if exists && old != nil && old != conn {
		if err := old.Close(); err != nil {
			logutil.Warnf("close superseded foreign connection %q: %v", handle, err)
		}
	}
}

// GetForeignConn returns the connection registered for handle.
func (ses *Session) GetForeignConn(handle string) (process.ForeignConn, bool) {
	ses.foreignConnMu.Lock()
	defer ses.foreignConnMu.Unlock()
	c, ok := ses.foreignConns[handle]
	return c, ok
}

// RemoveForeignConn detaches the connection for handle and returns it so the
// caller can close it.
func (ses *Session) RemoveForeignConn(handle string) (process.ForeignConn, bool) {
	ses.foreignConnMu.Lock()
	defer ses.foreignConnMu.Unlock()
	c, ok := ses.foreignConns[handle]
	if ok {
		delete(ses.foreignConns, handle)
	}
	return c, ok
}

// closeForeignConns closes and clears every cached foreign connection. It is
// called from Session.Close and is safe to call when no connection was ever
// opened (the map is lazily allocated).
func (ses *Session) closeForeignConns() {
	ses.foreignConnMu.Lock()
	conns := ses.foreignConns
	ses.foreignConns = nil
	ses.foreignConnMu.Unlock()

	for handle, c := range conns {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil {
			logutil.Warnf("close foreign connection %q on session close: %v", handle, err)
		}
	}
}
