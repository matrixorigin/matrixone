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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// maxForeignConns bounds the number of foreign connections one session may
// cache. Each entry owns real resources (a sql.DB pool or an HTTP transport),
// and entries live until explicit disconnect or session close, so admission
// must be bounded (Q3). The cap is per session and generous for interactive
// use; exceeding it is a usage error with an actionable message.
const maxForeignConns = 16

// Session implements process.ForeignConnCache so that esql_tvf / sql_tvf and
// their connect/disconnect builtins can cache foreign-data-source connections
// for the lifetime of the interactive session. All connections are closed by
// closeForeignConns when the session ends. Handles are derived from the
// connection config, so reconnecting with the same config reuses the entry.
var _ process.ForeignConnCache = (*Session)(nil)

// PutForeignConn stores conn under handle unless an entry already exists and
// returns the cached entry (first-wins). Handles are config-derived, so two
// concurrent scans with the same config can race to connect; keeping the
// first entry (instead of superseding) means the cache never closes a
// connection another operator may already be using — the loser closes its own.
func (ses *Session) PutForeignConn(ctx context.Context, handle string, conn process.ForeignConn) (process.ForeignConn, error) {
	ses.foreignConnMu.Lock()
	defer ses.foreignConnMu.Unlock()
	if ses.foreignConns == nil {
		ses.foreignConns = make(map[string]process.ForeignConn)
	}
	if existing, ok := ses.foreignConns[handle]; ok && existing != nil {
		return existing, nil
	}
	if len(ses.foreignConns) >= maxForeignConns {
		return nil, moerr.NewInvalidInputf(ctx,
			"this session already caches %d foreign connections; disconnect unused handles with esql_tvf_disconnect/sql_tvf_disconnect or reuse an existing config",
			maxForeignConns)
	}
	ses.foreignConns[handle] = conn
	return conn, nil
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
