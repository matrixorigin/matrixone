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

// Package datastream implements the planner-side pieces of the datastream
// external table (CREATE EXTERNAL TABLE ... ENGINE = DATASTREAM): option
// validation, the rel_createsql envelope, and the conservative filter
// deparser used for predicate pushdown.  See
// docs/cn/stream_transport_fast_upload.md.
package datastream

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	CreateSQLEnvelopePrefix = "MO_DATASTREAM:"
	CreateSQLKindDataStream = "datastream_table"

	optionServer  = "server"
	optionPort    = "port"
	optionTable   = "table"
	optionRecheck = "recheck"
	optionAPIKey  = "apikey"
)

// Config is the validated content of the WITH (...) option list.
type Config struct {
	Server string
	Port   int32
	Table  string
	// Recheck true (the default) means the pushed-down filter is a pure hint
	// and MO re-applies it locally; false trusts the server for the pushed
	// conjuncts.
	Recheck bool
	// APIKey is the optional shared secret presented to the server; "" means
	// the table configured no key.
	APIKey string
}

// Address returns the gRPC dial target. JoinHostPort brackets an IPv6 literal
// (::1 -> [::1]:port) instead of producing the ambiguous ::1:port.
func (c Config) Address() string {
	return net.JoinHostPort(c.Server, strconv.Itoa(int(c.Port)))
}

// ParseTableOptions validates the WITH (...) list of ENGINE = DATASTREAM.
func ParseTableOptions(ctx context.Context, param *tree.DataStreamTableParam) (Config, error) {
	cfg := Config{Recheck: true}
	seen := make(map[string]bool)
	for _, option := range param.Options {
		key := strings.ToLower(strings.TrimSpace(string(option.Key)))
		if seen[key] {
			return Config{}, moerr.NewInvalidInputf(ctx, "duplicate datastream option '%s'", key)
		}
		seen[key] = true
		value := strings.TrimSpace(option.Val)
		if value == "" {
			return Config{}, moerr.NewInvalidInputf(ctx, "datastream option '%s' must not be empty", key)
		}
		switch key {
		case optionServer:
			cfg.Server = value
		case optionPort:
			port, err := strconv.ParseUint(value, 10, 16)
			if err != nil || port == 0 {
				return Config{}, moerr.NewInvalidInputf(ctx, "datastream option 'port' must be a port number, got '%s'", value)
			}
			cfg.Port = int32(port)
		case optionTable:
			cfg.Table = value
		case optionRecheck:
			recheck, err := strconv.ParseBool(strings.ToLower(value))
			if err != nil {
				return Config{}, moerr.NewInvalidInputf(ctx, "datastream option 'recheck' must be true or false, got '%s'", value)
			}
			cfg.Recheck = recheck
		case optionAPIKey:
			cfg.APIKey = value
		default:
			return Config{}, moerr.NewInvalidInputf(ctx, "unknown datastream option '%s' (supported: server, port, table, recheck, apikey)", key)
		}
	}
	if cfg.Server == "" {
		return Config{}, moerr.NewInvalidInput(ctx, "datastream external table requires the 'server' option")
	}
	if cfg.Port == 0 {
		return Config{}, moerr.NewInvalidInput(ctx, "datastream external table requires the 'port' option")
	}
	if cfg.Table == "" {
		return Config{}, moerr.NewInvalidInput(ctx, "datastream external table requires the 'table' option")
	}
	return cfg, nil
}

// BuildCreateSQLEnvelope renders the config into the planner-owned
// rel_createsql comment envelope.
func BuildCreateSQLEnvelope(cfg Config) string {
	// The API key lives in the catalog-internal rel_createsql (never surfaced
	// by SHOW CREATE, see build_show_util.go). It is url-escaped like every
	// other field so ';' and '*/' in the secret cannot break the envelope.
	return fmt.Sprintf(
		"/* %s version=1; kind=%s; server=%s; port=%d; table=%s; recheck=%t; apikey=%s */",
		CreateSQLEnvelopePrefix,
		CreateSQLKindDataStream,
		url.QueryEscape(cfg.Server),
		cfg.Port,
		url.QueryEscape(cfg.Table),
		cfg.Recheck,
		url.QueryEscape(cfg.APIKey),
	)
}

// ParseCreateSQLEnvelope recognizes and parses the datastream envelope.  The
// bool result reports whether the string is a datastream envelope at all;
// rel_createsql of a generic external table is user-controlled JSON, so only
// the anchored leading comment is recognized (same rationale as the MongoDB
// envelope: searching the whole string would allow injection through a
// user-controlled filepath).
func ParseCreateSQLEnvelope(ctx context.Context, createSQL string) (Config, bool, error) {
	createSQL = strings.TrimSpace(createSQL)
	prefix := "/* " + CreateSQLEnvelopePrefix
	if !strings.HasPrefix(createSQL, prefix) {
		return Config{}, false, nil
	}
	start := len(prefix)
	end := strings.Index(createSQL[start:], "*/")
	if end < 0 {
		return Config{}, true, moerr.NewInvalidInput(ctx, "datastream rel_createsql envelope is not closed")
	}
	fields := make(map[string]string)
	for _, part := range strings.Split(createSQL[start:start+end], ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return Config{}, true, moerr.NewInvalidInput(ctx, "datastream rel_createsql envelope field must be key=value")
		}
		decoded, err := url.QueryUnescape(strings.TrimSpace(value))
		if err != nil {
			return Config{}, true, moerr.NewInvalidInput(ctx, "datastream rel_createsql envelope field is not url-escaped")
		}
		fields[strings.ToLower(strings.TrimSpace(key))] = decoded
	}
	if version, err := strconv.Atoi(fields["version"]); err != nil || version != 1 {
		return Config{}, true, moerr.NewInvalidInput(ctx, "datastream rel_createsql envelope version must be 1")
	}
	if fields["kind"] != CreateSQLKindDataStream {
		return Config{}, true, moerr.NewInvalidInput(ctx, "datastream rel_createsql envelope kind must be datastream_table")
	}
	port, err := strconv.ParseUint(fields["port"], 10, 16)
	if err != nil || port == 0 {
		return Config{}, true, moerr.NewInvalidInput(ctx, "datastream rel_createsql envelope has an invalid port")
	}
	recheck, err := strconv.ParseBool(fields["recheck"])
	if err != nil {
		return Config{}, true, moerr.NewInvalidInput(ctx, "datastream rel_createsql envelope has an invalid recheck flag")
	}
	cfg := Config{
		Server:  fields["server"],
		Port:    int32(port),
		Table:   fields["table"],
		Recheck: recheck,
		// apikey is absent from envelopes written before auth existed; "" then
		// means "no key", which matches a server that requires none
		APIKey: fields["apikey"],
	}
	if cfg.Server == "" || cfg.Table == "" {
		return Config{}, true, moerr.NewInvalidInput(ctx, "datastream rel_createsql envelope missing server or table")
	}
	return cfg, true, nil
}
