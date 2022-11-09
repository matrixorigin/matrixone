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
	"context"
)

// IConn is an interface of a connection.
type IConn interface {
	// Close closes the connection. It is the only function
	// which a underlying connection must implement.
	Close() error
}

// IConnPool is an interface of connection pool.
type IConnPool interface {
	// Acquire acquires a connection from the pool.
	Acquire(context.Context) (*Conn, error)
	// Release releases the connection and put it back into the pool.
	// NB: cannot release one connection more than one time.
	Release(*Conn)
	// Len returns the length of the connection pool.
	Len() int
	// Close closes the connection pool.
	Close() error
	// Stats returns the statistics of the connection pool.
	Stats() *Stats
}
