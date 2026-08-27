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

	optionConfig  = "config"
	optionQuery   = "query"
	optionRecheck = "recheck"
)

// Config is the validated content of the WITH (...) option list.
type Config struct {
	// Kind is KindESQL or KindSQL, from the ENGINE clause.
	Kind string
	// ConfigJSON is the connection config: inline JSON (as accepted by
	// esql_tvf_connect / sql_tvf_connect), or "" meaning "resolve from
	// @esql_tvf_config / @sql_tvf_config at scan time". User input or session
	// only; query processing never reads the CN process environment.
	ConfigJSON string
	// DefaultQuery is the query text used when a SELECT has no
	// __mo_query = '...' predicate; "" means no default.
	DefaultQuery string
	// Recheck true (the default) means MO applies every predicate itself and
	// sends the query text verbatim; false opts into predicate pushdown,
	// wrapping the query so the remote evaluates the deparsable conjuncts.
	// SQL only -- ENGINE = ESQL rejects the option.
	Recheck bool
}

// ParseTableOptions validates the WITH (...) list of ENGINE = ESQL|SQL.
func ParseTableOptions(ctx context.Context, param *tree.ForeignTableParam) (Config, error) {
	cfg := Config{Kind: strings.ToLower(param.Kind), Recheck: true}
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
					"the 'config' option is '<redacted>' (SHOW CREATE hides inline credentials); re-supply the real config, or omit it and set the @esql_tvf_config / @sql_tvf_config session variable")
			}
			cfg.ConfigJSON = value
		case optionQuery:
			cfg.DefaultQuery = value
		case optionRecheck:
			// Pushdown wraps the user's query text as a MySQL-dialect
			// derived table; ES|QL has no such form, so ESQL stays on the
			// verbatim path until it grows a pushdown of its own.
			if cfg.Kind != KindSQL {
				return Config{}, moerr.NewInvalidInputf(ctx,
					"the 'recheck' option is only supported by ENGINE = SQL, not %s", cfg.Kind)
			}
			recheck, err := strconv.ParseBool(strings.ToLower(value))
			if err != nil {
				return Config{}, moerr.NewInvalidInputf(ctx,
					"%s option 'recheck' must be true or false, got '%s'", cfg.Kind, value)
			}
			cfg.Recheck = recheck
		default:
			supported := "config, query"
			if cfg.Kind == KindSQL {
				supported = "config, query, recheck"
			}
			return Config{}, moerr.NewInvalidInputf(ctx, "unknown %s option '%s' (supported: %s)", cfg.Kind, key, supported)
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
		"/* %s version=1; kind=%s; engine=%s; config=%s; query=%s; recheck=%t */",
		CreateSQLEnvelopePrefix,
		CreateSQLKindForeign,
		url.QueryEscape(cfg.Kind),
		url.QueryEscape(cfg.ConfigJSON),
		url.QueryEscape(cfg.DefaultQuery),
		cfg.Recheck,
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
		// recheck is absent from envelopes written before pushdown existed;
		// those tables were read with every predicate applied locally, which
		// is exactly recheck=true -- so absent must decode as the default,
		// never as the bool zero value.  (The reverse direction is safe too:
		// an older binary ignores the field it does not know.)
		Recheck: true,
	}
	if raw, ok := fields["recheck"]; ok {
		recheck, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, true, moerr.NewInvalidInput(ctx, "foreign table rel_createsql envelope has an invalid recheck flag")
		}
		cfg.Recheck = recheck
	}
	if cfg.Kind != KindESQL && cfg.Kind != KindSQL {
		return Config{}, true, moerr.NewInvalidInput(ctx, "foreign table rel_createsql envelope has an invalid engine")
	}
	return cfg, true, nil
}
