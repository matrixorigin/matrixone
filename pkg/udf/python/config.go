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

package python

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// maxHandlerTimeout is part of the current Gateway/worker payload contract.
// The worker rejects a handler timeout above one hour; reject it while loading
// the CN configuration so an enabled runtime cannot appear healthy and then
// fail every invocation at the Flight boundary.
const maxHandlerTimeout = time.Hour

type Config struct {
	UUID    string `toml:"uuid"`
	Address string `toml:"address"`
	Path    string `toml:"path"`
	Python  string `toml:"python"`
}

func (c *Config) Validate() error {
	if c.Address == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf address")
	}
	if c.Path == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf path")
	}
	return nil
}

type ClientConfig struct {
	// Enabled is deliberately false by default.  The Python worker is an
	// unisolated development adapter until the production sandbox contract is
	// deployed, so a generic CN launch must never attach it implicitly.
	Enabled         bool   `toml:"enabled"`
	AllowUnisolated bool   `toml:"allow-unisolated"`
	ServerAddress   string `toml:"server-address"`
	MaxBatchBytes   int64  `toml:"max-batch-bytes"`
	MaxBatchRows    int64  `toml:"max-batch-rows"`
	// MaxInvocationRows bounds the rows retained by one physical invocation.
	// It is checked before OpenInvocation so a large request cannot reserve a
	// worker or run user code before the CN has accepted its result shape.
	MaxInvocationRows int64 `toml:"max-invocation-rows"`
	// MaxInvocationResultBytes bounds the retained MO result vector for one
	// invocation.  Arrow frames remain subject to MaxBatchBytes; this separate
	// limit prevents many individually valid batches from accumulating an
	// unbounded result in the Gateway.
	MaxInvocationResultBytes int64 `toml:"max-invocation-result-bytes"`
	// MaxActiveInvocations is K: a CN refuses a new physical invocation when
	// all slots are in use. It deliberately does not queue, because queued
	// execution would retain the caller's input vectors and their allocation
	// accounts while waiting for a worker slot.
	MaxActiveInvocations int           `toml:"max-active-invocations"`
	RequestTimeout       time.Duration `toml:"request-timeout"`
	// Terminal limits cover both active and retained completion tombstones.
	// They are reserved before an invocation is sent, so a long stream of
	// short calls cannot grow the deduplication ledger without bound.
	MaxTerminalEntries int           `toml:"max-terminal-entries"`
	MaxTerminalBytes   int64         `toml:"max-terminal-bytes"`
	TerminalRecordTTL  time.Duration `toml:"terminal-record-ttl"`
}

func (c *ClientConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if !c.AllowUnisolated {
		return moerr.NewInternalError(context.Background(), "python udf requires explicit allow-unisolated until the sandbox is configured")
	}
	if c.ServerAddress == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf address")
	}
	if c.MaxBatchBytes < 0 || c.MaxBatchBytes > 1<<30 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max batch bytes")
	}
	if c.MaxBatchRows < 0 || c.MaxBatchRows > 1<<30 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max batch rows")
	}
	if c.MaxInvocationRows < 0 || c.MaxInvocationRows > 1<<32 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max invocation rows")
	}
	if c.MaxInvocationResultBytes < 0 || c.MaxInvocationResultBytes > 1<<40 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max invocation result bytes")
	}
	if c.MaxActiveInvocations < 0 || c.MaxActiveInvocations > 1<<20 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max active invocations")
	}
	if c.RequestTimeout < 0 || c.RequestTimeout > maxHandlerTimeout {
		return moerr.NewInternalError(context.Background(), "invalid python udf request timeout")
	}
	if c.MaxTerminalEntries < 0 || c.MaxTerminalEntries > 1<<30 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max terminal entries")
	}
	if c.MaxTerminalBytes < 0 || c.MaxTerminalBytes > 1<<40 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max terminal bytes")
	}
	if c.TerminalRecordTTL < 0 {
		return moerr.NewInternalError(context.Background(), "invalid python udf terminal record ttl")
	}
	return nil
}
