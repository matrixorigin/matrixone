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

package datastream

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func options(kv ...string) *tree.DataStreamTableParam {
	param := &tree.DataStreamTableParam{}
	for i := 0; i < len(kv); i += 2 {
		param.Options = append(param.Options, tree.NewDataStreamOption(tree.Identifier(kv[i]), kv[i+1]))
	}
	return param
}

func TestParseTableOptions(t *testing.T) {
	ctx := context.Background()

	cfg, err := ParseTableOptions(ctx, options("server", "10.0.0.1", "port", "4444", "table", "src"))
	require.NoError(t, err)
	require.Equal(t, Config{Server: "10.0.0.1", Port: 4444, Table: "src", Recheck: true}, cfg)
	require.Equal(t, "10.0.0.1:4444", cfg.Address())

	cfg, err = ParseTableOptions(ctx, options("Server", "h", "PORT", "1", "table", "t", "recheck", "false"))
	require.NoError(t, err)
	require.False(t, cfg.Recheck)

	// apikey is optional; when present it is carried on the config
	cfg, err = ParseTableOptions(ctx, options("server", "h", "port", "1", "table", "t", "apikey", "s3cr3t"))
	require.NoError(t, err)
	require.Equal(t, "s3cr3t", cfg.APIKey)

	for _, bad := range []*tree.DataStreamTableParam{
		options("port", "4444", "table", "t"),                                 // missing server
		options("server", "h", "table", "t"),                                  // missing port
		options("server", "h", "port", "4444"),                                // missing table
		options("server", "h", "port", "0", "table", "t"),                     // port 0
		options("server", "h", "port", "99999", "table", "t"),                 // port overflow
		options("server", "h", "port", "abc", "table", "t"),                   // port not a number
		options("server", "h", "port", "4444", "table", "t", "recheck", "xx"), // bad recheck
		options("server", "h", "port", "4444", "table", "t", "server", "h2"),  // duplicate
		options("server", "h", "port", "4444", "table", "t", "bogus", "v"),    // unknown key
		options("server", "", "port", "4444", "table", "t"),                   // empty value
	} {
		_, err := ParseTableOptions(ctx, bad)
		require.Error(t, err)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	// an api key containing envelope-breaking bytes (';', '*/', '%') round-trips
	in := Config{Server: "my host%;*/weird", Port: 65535, Table: "the;table=1", Recheck: false, APIKey: "k;e*/y%z"}
	envelope := BuildCreateSQLEnvelope(in)

	out, found, err := ParseCreateSQLEnvelope(ctx, envelope)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, in, out)

	// an envelope written before auth existed (no apikey field) parses with an
	// empty key rather than failing
	legacy := "/* MO_DATASTREAM: version=1; kind=datastream_table; server=h; port=1; table=t; recheck=true */"
	out, found, err = ParseCreateSQLEnvelope(ctx, legacy)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "", out.APIKey)

	// non-envelope content is not recognized, not an error
	for _, notEnv := range []string{"", "{\"ScanType\":0}", "create table t(a int)", "/* MO_MONGODB: ... */"} {
		_, found, err = ParseCreateSQLEnvelope(ctx, notEnv)
		require.NoError(t, err)
		require.False(t, found)
	}

	// recognized but malformed envelopes are errors
	for _, badEnv := range []string{
		"/* MO_DATASTREAM: version=1", // unclosed
		"/* MO_DATASTREAM: version=2; kind=datastream_table; server=h; port=1; table=t; recheck=true */", // bad version
		"/* MO_DATASTREAM: version=1; kind=other; server=h; port=1; table=t; recheck=true */",            // bad kind
		"/* MO_DATASTREAM: version=1; kind=datastream_table; server=h; port=x; table=t; recheck=true */", // bad port
		"/* MO_DATASTREAM: version=1; kind=datastream_table; server=h; port=1; table=t; recheck=zz */",   // bad recheck
		"/* MO_DATASTREAM: version=1; kind=datastream_table; server=; port=1; table=t; recheck=true */",  // no server
		"/* MO_DATASTREAM: noequalsign */",           // not key=value
		"/* MO_DATASTREAM: server=%zz; version=1 */", // bad escape
	} {
		_, found, err = ParseCreateSQLEnvelope(ctx, badEnv)
		require.True(t, found, badEnv)
		require.Error(t, err, badEnv)
	}
}
