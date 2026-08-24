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

package kafka

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func options(kv ...string) *tree.KafkaTableParam {
	param := &tree.KafkaTableParam{}
	for i := 0; i < len(kv); i += 2 {
		param.Options = append(param.Options, tree.NewKafkaTableOption(tree.Identifier(kv[i]), kv[i+1]))
	}
	return param
}

func TestParseTableOptions(t *testing.T) {
	ctx := context.Background()

	cfg, err := ParseTableOptions(ctx, options("brokers", "h1:9092,h2:9092", "topic", "t1"))
	require.NoError(t, err)
	require.Equal(t, Config{Brokers: "h1:9092,h2:9092", Topic: "t1", Partition: 0,
		Autocommit: false, Format: FormatCSV, Separator: ","}, cfg)

	cfg, err = ParseTableOptions(ctx, options(
		"Brokers", "[::1]:9092", "TOPIC", "t", "partition", "3",
		"autocommit", "TRUE", "group", "g1", "format", "JSONL"))
	require.NoError(t, err)
	require.Equal(t, int32(3), cfg.Partition)
	require.True(t, cfg.Autocommit)
	require.Equal(t, "g1", cfg.Group)
	require.Equal(t, FormatJSONL, cfg.Format)

	// csv separator: verbatim single rune, including whitespace and multibyte
	for _, sep := range []string{"|", "\t", " ", "§"} {
		cfg, err = ParseTableOptions(ctx, options("brokers", "h:1", "topic", "t", "separator", sep))
		require.NoError(t, err, sep)
		require.Equal(t, sep, cfg.Separator)
	}

	for _, bad := range []*tree.KafkaTableParam{
		options("topic", "t"),                          // missing brokers
		options("brokers", "h:9092"),                   // missing topic
		options("brokers", "nohostport", "topic", "t"), // broker not host:port
		options("brokers", "h:9092,alsobad", "topic", "t"),
		options("brokers", "h:9092", "topic", "t", "partition", "-1"),
		options("brokers", "h:9092", "topic", "t", "partition", "x"),
		options("brokers", "h:9092", "topic", "t", "autocommit", "maybe"),
		options("brokers", "h:9092", "topic", "t", "format", "xml"),
		options("brokers", "h:9092", "topic", "t", "separator", "||"),                   // two runes
		options("brokers", "h:9092", "topic", "t", "format", "jsonl", "separator", "|"), // sep with jsonl
		options("brokers", "h:9092", "topic", "t", "topic", "t2"),                       // duplicate
		options("brokers", "h:9092", "topic", "t", "bogus", "v"),                        // unknown
		options("brokers", "", "topic", "t"),                                            // empty value
	} {
		_, err := ParseTableOptions(ctx, bad)
		require.Error(t, err)
	}
}

func TestDefaultGroup(t *testing.T) {
	require.Equal(t, "mo_kafka_db1_t1", DefaultGroup("db1", "t1"))
}

func TestEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	in := Config{
		Brokers: "h;1:9092,h*/2:9092", Topic: "the;topic=1", Partition: 7,
		Autocommit: true, Group: "g%;*/x", Format: FormatCSV, Separator: "\t",
	}
	envelope := BuildCreateSQLEnvelope(in)
	out, found, err := ParseCreateSQLEnvelope(ctx, envelope)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, in, out)

	// non-envelope content is not recognized, not an error
	for _, notEnv := range []string{"", "{\"ScanType\":0}", "create table t(a int)", "/* MO_DATASTREAM: ... */"} {
		_, found, err = ParseCreateSQLEnvelope(ctx, notEnv)
		require.NoError(t, err)
		require.False(t, found)
	}

	// recognized but malformed envelopes are errors
	for _, badEnv := range []string{
		"/* MO_KAFKA: version=1", // unclosed
		"/* MO_KAFKA: version=2; kind=kafka_table; brokers=h%3A1; topic=t; partition=0; autocommit=false; group=g; format=csv; separator=%2C */",
		"/* MO_KAFKA: version=1; kind=other; brokers=h%3A1; topic=t; partition=0; autocommit=false; group=g; format=csv; separator=%2C */",
		"/* MO_KAFKA: version=1; kind=kafka_table; brokers=h%3A1; topic=t; partition=x; autocommit=false; group=g; format=csv; separator=%2C */",
		"/* MO_KAFKA: version=1; kind=kafka_table; brokers=h%3A1; topic=t; partition=0; autocommit=zz; group=g; format=csv; separator=%2C */",
		"/* MO_KAFKA: version=1; kind=kafka_table; brokers=; topic=t; partition=0; autocommit=false; group=g; format=csv; separator=%2C */",
		"/* MO_KAFKA: version=1; kind=kafka_table; brokers=h%3A1; topic=t; partition=0; autocommit=false; group=; format=csv; separator=%2C */",
		"/* MO_KAFKA: version=1; kind=kafka_table; brokers=h%3A1; topic=t; partition=0; autocommit=false; group=g; format=xml; separator=%2C */",
		"/* MO_KAFKA: noequalsign */",
		"/* MO_KAFKA: brokers=%zz; version=1 */", // bad escape
	} {
		_, found, err = ParseCreateSQLEnvelope(ctx, badEnv)
		require.True(t, found, badEnv)
		require.Error(t, err, badEnv)
	}

	// an empty separator in a hand-written envelope falls back to ","
	out, found, err = ParseCreateSQLEnvelope(ctx,
		"/* MO_KAFKA: version=1; kind=kafka_table; brokers=h%3A1; topic=t; partition=0; autocommit=false; group=g; format=csv; separator= */")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ",", out.Separator)
}
