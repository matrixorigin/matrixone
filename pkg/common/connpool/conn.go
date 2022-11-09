// Copyright 2021 - 2022 Matrix Origin
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

package connpool

import (
	"time"
)

// Conn is the connection stored in the pool. It contains an
// underlying conn which is an interface.
type Conn struct {
	conn      IConn
	pooled    bool
	createdAt time.Time
	usedAt    time.Time
}

var _ IConn = (*Conn)(nil)

// NewConn creates a new conn whose createdAt is set to now.
func NewConn(conn IConn, pooled bool) *Conn {
	return &Conn{
		conn:      conn,
		pooled:    pooled,
		createdAt: time.Now(),
	}
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// UsedAt returns the time when the connection is last used.
func (c *Conn) UsedAt() time.Time {
	return c.usedAt
}

// Use set the usedAt time.
func (c *Conn) Use(t time.Time) {
	c.usedAt = t
}
