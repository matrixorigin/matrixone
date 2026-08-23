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

// Package foreigntvf implements the foreign-data connections behind the
// esql_tvf (Elasticsearch ES|QL) and sql_tvf (database/sql) table functions
// and their connect/disconnect builtins. A connection is opened once, cached
// on the interactive session (see process.ForeignConnCache), reused for the
// same config, and closed when the session ends. Each connection runs a query
// and returns its result as a CSV byte stream that the external CSV reader
// materializes into batches.
package foreigntvf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Kind identifies a foreign connection type.
type Kind string

const (
	KindESQL Kind = "esql"
	KindSQL  Kind = "sql"

	// SessionVarESQLConfig / SessionVarSQLConfig are the user session variables
	// consulted when a TVF/connect call omits an explicit config or handle.
	SessionVarESQLConfig = "esql_tvf_config"
	SessionVarSQLConfig  = "sql_tvf_config"
)

// Conn is a cached connection to a foreign data source. It satisfies
// process.ForeignConn (Close), so the session can own and close it.
type Conn interface {
	// Close releases the connection. Safe to call more than once.
	Close() error
	// Query runs queryText against the source and returns its result as a CSV
	// byte stream. The caller owns and closes the returned stream.
	Query(ctx context.Context, queryText string) (io.ReadCloser, error)
	// Kind reports the connection type, which selects the CSV dialect.
	Kind() Kind
}

var _ process.ForeignConn = (Conn)(nil)

// MakeHandle derives the session cache handle for a (kind, config) pair. The
// handle is deterministic, so reconnecting with the same config reuses the
// cached connection. The config JSON is compacted first, so whitespace-only
// variants of the same config map to one handle (and one cached connection);
// non-JSON input hashes as-is.
func MakeHandle(kind Kind, configJSON string) string {
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(configJSON)); err == nil {
		configJSON = compact.String()
	}
	sum := sha256.Sum256([]byte(configJSON))
	return string(kind) + ":" + hex.EncodeToString(sum[:8])
}

// configEnvPrefix marks a config value as a reference to a process
// environment variable resolved on the CN at connect time. Using it keeps the
// credential-bearing JSON out of SQL text entirely — statement logs, query
// history, and session variables then only ever see "env:NAME". This is the
// same convention the ESQL/SQL external tables and the datastream apikey use.
const configEnvPrefix = "env:"

// resolveConfigRef resolves an "env:NAME" config reference from the process
// environment; any other value is returned literally.
func resolveConfigRef(ctx context.Context, raw string) (string, error) {
	if !strings.HasPrefix(raw, configEnvPrefix) {
		return raw, nil
	}
	name := strings.TrimPrefix(raw, configEnvPrefix)
	if name == "" {
		return "", moerr.NewInvalidInput(ctx, "foreigntvf: config 'env:' reference has no variable name")
	}
	value := os.Getenv(name)
	if value == "" {
		return "", moerr.NewInvalidInputf(ctx, "foreigntvf: config env var %q is unset or empty", name)
	}
	return value, nil
}

// ValidateConfig checks the JSON shape of a connection config without dialing
// anything. Used at CREATE EXTERNAL TABLE time (docs/cn/esql_sql_exttab.md §4:
// no DDL-time connectivity check, only option/config syntax).
func ValidateConfig(ctx context.Context, kind Kind, configJSON string) error {
	switch kind {
	case KindESQL:
		return validateESQLConfig(ctx, configJSON)
	case KindSQL:
		return validateSQLConfig(ctx, configJSON)
	default:
		return moerr.NewInternalErrorf(ctx, "foreigntvf: unknown connection kind %q", kind)
	}
}

// Connect opens a new connection of the given kind from a JSON config string.
func Connect(ctx context.Context, kind Kind, configJSON string) (Conn, error) {
	switch kind {
	case KindESQL:
		return connectESQL(ctx, configJSON)
	case KindSQL:
		return connectSQL(ctx, configJSON)
	default:
		return nil, moerr.NewInternalErrorf(ctx, "foreigntvf: unknown connection kind %q", kind)
	}
}

// ResolveOrConnect returns the session-cached connection for configJSON,
// opening and caching a new one (keyed by a config-derived handle) if absent.
// It is used by the connect builtins and by a TVF whose conn argument is NULL.
// An "env:NAME" reference is resolved first and the handle is derived from
// the RESOLVED config, so a TVF connecting via env: and an external table
// whose option resolved to the same JSON share one cached connection.
func ResolveOrConnect(ctx context.Context, cache process.ForeignConnCache, kind Kind, configJSON string) (Conn, string, error) {
	configJSON, err := resolveConfigRef(ctx, configJSON)
	if err != nil {
		return nil, "", err
	}
	handle := MakeHandle(kind, configJSON)
	if c, ok := cache.GetForeignConn(handle); ok {
		if fc, ok := c.(Conn); ok {
			return fc, handle, nil
		}
	}
	c, err := Connect(ctx, kind, configJSON)
	if err != nil {
		return nil, "", err
	}
	// First-wins bounded admission: another operator may have connected
	// concurrently under the same config-derived handle, and the session caps
	// how many connections it retains. In both non-winning cases our freshly
	// opened conn must be closed — the cache never closes a connection someone
	// else may be using.
	winner, admitErr := cache.PutForeignConn(handle, c)
	if admitErr != nil {
		_ = c.Close()
		return nil, "", admitErr
	}
	if winner != process.ForeignConn(c) {
		_ = c.Close()
		if fc, ok := winner.(Conn); ok {
			return fc, handle, nil
		}
		return nil, "", moerr.NewInternalErrorf(ctx, "foreigntvf: cached handle %q is not a foreign connection", handle)
	}
	return c, handle, nil
}

// ByHandle returns the cached connection for an explicit handle, erroring if it
// was never opened or has been disconnected.
func ByHandle(ctx context.Context, cache process.ForeignConnCache, handle string) (Conn, error) {
	c, ok := cache.GetForeignConn(handle)
	if !ok {
		return nil, moerr.NewInvalidInputf(ctx, "foreigntvf: connection handle %q not found or disconnected; check the conn argument", handle)
	}
	fc, ok := c.(Conn)
	if !ok {
		return nil, moerr.NewInternalErrorf(ctx, "foreigntvf: cached handle %q is not a foreign connection", handle)
	}
	return fc, nil
}

// ConfigFromSessionVar reads the default-config user session variable for a
// kind (SessionVarESQLConfig / SessionVarSQLConfig). It errors if the variable
// is unset or empty, since there is then no way to establish a connection.
func ConfigFromSessionVar(ctx context.Context, proc *process.Process, kind Kind) (string, error) {
	varName := SessionVarESQLConfig
	if kind == KindSQL {
		varName = SessionVarSQLConfig
	}
	resolve := proc.GetResolveVariableFunc()
	if resolve == nil {
		return "", moerr.NewInvalidInputf(ctx, "foreigntvf: no connection given and session variable @%s is unavailable", varName)
	}
	v, err := resolve(varName, false, false)
	if err != nil {
		return "", err
	}
	s, ok := valueToString(v)
	if !ok {
		return "", moerr.NewInvalidInputf(ctx, "foreigntvf: session variable @%s must be a string config, not %T", varName, v)
	}
	if s == "" {
		return "", moerr.NewInvalidInputf(ctx, "foreigntvf: no connection given and session variable @%s is not set", varName)
	}
	return s, nil
}

// valueToString converts a session-variable value into a config string;
// ok=false means the variable holds a non-string value.
func valueToString(v any) (string, bool) {
	switch t := v.(type) {
	case nil:
		return "", true
	case string:
		return t, true
	case []byte:
		return string(t), true
	default:
		return "", false
	}
}
