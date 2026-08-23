// Copyright 2026 Matrix Origin
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

// Package foreignext implements the planner-side pieces of the ESQL / SQL
// foreign external tables (CREATE EXTERNAL TABLE ... ENGINE = ESQL|SQL):
// option validation and the rel_createsql envelope.  Execution reuses
// pkg/sql/foreigntvf (connections, query -> CSV stream) and the external CSV
// reader.  See docs/cn/esql_sql_exttab.md.
package foreignext

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	CreateSQLEnvelopePrefix = "MO_FOREIGN:"
	CreateSQLKindForeign    = "foreign_table"

	// KindESQL / KindSQL name the engine; they equal foreigntvf.KindESQL /
	// foreigntvf.KindSQL (asserted by a unit test; this package avoids the
	// foreigntvf import so the planner does not link the DB client libraries).
	KindESQL = "esql"
	KindSQL  = "sql"

	optionConfig = "config"
	optionQuery  = "query"

	// configEnvPrefix marks a config value as a reference to a process
	// environment variable resolved on the CN at scan time, instead of inline
	// JSON carrying credentials. Using it keeps the secret out of the catalog
	// (rel_createsql), the plan, and statement logs.
	configEnvPrefix = "env:"
)

// Config is the validated content of the WITH (...) option list.
type Config struct {
	// Kind is KindESQL or KindSQL, from the ENGINE clause.
	Kind string
	// ConfigJSON is the connection config: inline JSON (as accepted by
	// esql_tvf_connect / sql_tvf_connect), an "env:NAME" reference, or ""
	// meaning "resolve from @esql_tvf_config / @sql_tvf_config at scan time".
	ConfigJSON string
	// DefaultQuery is the query text used when a SELECT has no
	// __mo_query = '...' predicate; "" means no default.
	DefaultQuery string
}

// ResolveConfig resolves the config option value into the connection config
// JSON. A value "env:NAME" is resolved from the process environment at scan
// time; any other value is used literally. "" stays "" (caller falls back to
// the session variable).
func ResolveConfig(ctx context.Context, raw string) (string, error) {
	if !strings.HasPrefix(raw, configEnvPrefix) {
		return raw, nil
	}
	name := strings.TrimPrefix(raw, configEnvPrefix)
	if name == "" {
		return "", moerr.NewInvalidInput(ctx, "foreign table config 'env:' reference has no variable name")
	}
	value := os.Getenv(name)
	if value == "" {
		return "", moerr.NewInvalidInputf(ctx, "foreign table config env var %q is unset or empty", name)
	}
	return value, nil
}

// ParseTableOptions validates the WITH (...) list of ENGINE = ESQL|SQL.
func ParseTableOptions(ctx context.Context, param *tree.ForeignTableParam) (Config, error) {
	cfg := Config{Kind: strings.ToLower(param.Kind)}
	if cfg.Kind != KindESQL && cfg.Kind != KindSQL {
		return Config{}, moerr.NewInvalidInputf(ctx, "unknown foreign external table engine '%s'", param.Kind)
	}
	seen := make(map[string]bool)
	for _, option := range param.Options {
		key := strings.ToLower(strings.TrimSpace(string(option.Key)))
		if seen[key] {
			return Config{}, moerr.NewInvalidInputf(ctx, "duplicate %s option '%s'", cfg.Kind, key)
		}
		seen[key] = true
		value := strings.TrimSpace(option.Val)
		if value == "" {
			return Config{}, moerr.NewInvalidInputf(ctx, "%s option '%s' must not be empty", cfg.Kind, key)
		}
		switch key {
		case optionConfig:
			if value == "<redacted>" {
				// SHOW CREATE redacts an inline config; replaying its output
				// (snapshot/PITR restore, copy-paste) must say why it cannot
				// work instead of failing on unparseable JSON.
				return Config{}, moerr.NewInvalidInputf(ctx,
					"the 'config' option is '<redacted>' (SHOW CREATE hides inline credentials); re-supply the real config, or use an env:NAME reference which is never redacted")
			}
			cfg.ConfigJSON = value
		case optionQuery:
			cfg.DefaultQuery = value
		default:
			return Config{}, moerr.NewInvalidInputf(ctx, "unknown %s option '%s' (supported: config, query)", cfg.Kind, key)
		}
	}
	return cfg, nil
}

// BuildCreateSQLEnvelope renders the config into the planner-owned
// rel_createsql comment envelope.
func BuildCreateSQLEnvelope(cfg Config) string {
	// An inline config may carry credentials; it lives only in the
	// catalog-internal rel_createsql (SHOW CREATE redacts it, see
	// build_show_util.go). Every field is url-escaped so ';' and '*/' in a DSN
	// cannot break the envelope.
	return fmt.Sprintf(
		"/* %s version=1; kind=%s; engine=%s; config=%s; query=%s */",
		CreateSQLEnvelopePrefix,
		CreateSQLKindForeign,
		url.QueryEscape(cfg.Kind),
		url.QueryEscape(cfg.ConfigJSON),
		url.QueryEscape(cfg.DefaultQuery),
	)
}

// ParseCreateSQLEnvelope recognizes and parses the foreign-table envelope.
// The bool result reports whether the string is such an envelope at all;
// rel_createsql of a generic external table is user-controlled JSON, so only
// the anchored leading comment is recognized (same anti-forgery rationale as
// the datastream envelope, and the feature bit must agree besides).
func ParseCreateSQLEnvelope(ctx context.Context, createSQL string) (Config, bool, error) {
	createSQL = strings.TrimSpace(createSQL)
	prefix := "/* " + CreateSQLEnvelopePrefix
	if !strings.HasPrefix(createSQL, prefix) {
		return Config{}, false, nil
	}
	start := len(prefix)
	end := strings.Index(createSQL[start:], "*/")
	if end < 0 {
		return Config{}, true, moerr.NewInvalidInput(ctx, "foreign table rel_createsql envelope is not closed")
	}
	fields := make(map[string]string)
	for _, part := range strings.Split(createSQL[start:start+end], ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return Config{}, true, moerr.NewInvalidInput(ctx, "foreign table rel_createsql envelope field must be key=value")
		}
		decoded, err := url.QueryUnescape(strings.TrimSpace(value))
		if err != nil {
			return Config{}, true, moerr.NewInvalidInput(ctx, "foreign table rel_createsql envelope field is not url-escaped")
		}
		fields[strings.ToLower(strings.TrimSpace(key))] = decoded
	}
	if version, err := strconv.Atoi(fields["version"]); err != nil || version != 1 {
		return Config{}, true, moerr.NewInvalidInput(ctx, "foreign table rel_createsql envelope version must be 1")
	}
	if fields["kind"] != CreateSQLKindForeign {
		return Config{}, true, moerr.NewInvalidInput(ctx, "foreign table rel_createsql envelope kind must be foreign_table")
	}
	cfg := Config{
		Kind:         fields["engine"],
		ConfigJSON:   fields["config"],
		DefaultQuery: fields["query"],
	}
	if cfg.Kind != KindESQL && cfg.Kind != KindSQL {
		return Config{}, true, moerr.NewInvalidInput(ctx, "foreign table rel_createsql envelope has an invalid engine")
	}
	return cfg, true, nil
}
