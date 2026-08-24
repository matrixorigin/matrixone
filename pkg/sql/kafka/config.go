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

// Package kafka implements the planner-side pieces of the Kafka external
// table (CREATE EXTERNAL TABLE ... ENGINE = KAFKA): option validation and the
// rel_createsql envelope. See docs/cn/kafka_exttab.md and issue #27518.
package kafka

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	CreateSQLEnvelopePrefix = "MO_KAFKA:"
	CreateSQLKindKafka      = "kafka_table"

	optionBrokers    = "brokers"
	optionTopic      = "topic"
	optionPartition  = "partition"
	optionAutocommit = "autocommit"
	optionGroup      = "group"
	optionFormat     = "format"
	optionSeparator  = "separator"

	FormatCSV   = "csv"
	FormatJSONL = "jsonl"
)

// Config is the validated content of the WITH (...) option list.
type Config struct {
	// Brokers is the bootstrap list, comma-separated host:port entries.
	Brokers string
	// Topic is the Kafka topic to read.
	Topic string
	// Partition is the single partition this table reads (default 0).
	Partition int32
	// Autocommit false (the default) means every SELECT must pin its start
	// position with __mo_read_start_id and the read offset is committed at
	// that position; true reads from earliest/latest/committed progress and
	// commits as the scan completes.
	Autocommit bool
	// Group is the consumer group whose committed offset is the exactly-once
	// bookmark. "" at option-parse time; build_ddl fills the default
	// ("mo_kafka_<db>_<table>") so the envelope always carries a concrete
	// group.
	Group string
	// Format is FormatCSV or FormatJSONL; each Kafka message value must parse
	// as exactly one record of this format.
	Format string
	// Separator is the CSV field separator (one rune, default ",").
	Separator string
}

// ParseTableOptions validates the WITH (...) list of ENGINE = KAFKA.
func ParseTableOptions(ctx context.Context, param *tree.KafkaTableParam) (Config, error) {
	cfg := Config{Partition: 0, Autocommit: false, Format: FormatCSV, Separator: ","}
	seen := make(map[string]bool)
	sepSet := false
	for _, option := range param.Options {
		key := strings.ToLower(strings.TrimSpace(string(option.Key)))
		if seen[key] {
			return Config{}, moerr.NewInvalidInputf(ctx, "duplicate kafka option '%s'", key)
		}
		seen[key] = true
		value := strings.TrimSpace(option.Val)
		if value == "" && key != optionSeparator {
			return Config{}, moerr.NewInvalidInputf(ctx, "kafka option '%s' must not be empty", key)
		}
		switch key {
		case optionBrokers:
			cfg.Brokers = value
		case optionTopic:
			cfg.Topic = value
		case optionPartition:
			p, err := strconv.ParseInt(value, 10, 32)
			if err != nil || p < 0 {
				return Config{}, moerr.NewInvalidInputf(ctx, "kafka option 'partition' must be a non-negative integer, got '%s'", value)
			}
			cfg.Partition = int32(p)
		case optionAutocommit:
			b, err := strconv.ParseBool(strings.ToLower(value))
			if err != nil {
				return Config{}, moerr.NewInvalidInputf(ctx, "kafka option 'autocommit' must be true or false, got '%s'", value)
			}
			cfg.Autocommit = b
		case optionGroup:
			cfg.Group = value
		case optionFormat:
			f := strings.ToLower(value)
			if f != FormatCSV && f != FormatJSONL {
				return Config{}, moerr.NewInvalidInputf(ctx, "kafka option 'format' must be csv or jsonl, got '%s'", value)
			}
			cfg.Format = f
		case optionSeparator:
			// option.Val is used verbatim (not trimmed): a tab or space
			// separator is legal
			cfg.Separator = option.Val
			sepSet = true
		default:
			return Config{}, moerr.NewInvalidInputf(ctx,
				"unknown kafka option '%s' (supported: brokers, topic, partition, autocommit, group, format, separator)", key)
		}
	}
	if cfg.Brokers == "" {
		return Config{}, moerr.NewInvalidInput(ctx, "kafka external table requires the 'brokers' option")
	}
	for _, b := range strings.Split(cfg.Brokers, ",") {
		if _, _, err := net.SplitHostPort(strings.TrimSpace(b)); err != nil {
			return Config{}, moerr.NewInvalidInputf(ctx, "kafka option 'brokers' entry '%s' is not host:port", strings.TrimSpace(b))
		}
	}
	if cfg.Topic == "" {
		return Config{}, moerr.NewInvalidInput(ctx, "kafka external table requires the 'topic' option")
	}
	if sepSet {
		if cfg.Format != FormatCSV {
			return Config{}, moerr.NewInvalidInput(ctx, "kafka option 'separator' is only valid with format csv")
		}
		if utf8.RuneCountInString(cfg.Separator) != 1 {
			return Config{}, moerr.NewInvalidInputf(ctx, "kafka option 'separator' must be a single character, got '%s'", cfg.Separator)
		}
	}
	return cfg, nil
}

// DefaultGroup is the consumer group used when the DDL sets none: stable per
// table so repeated sessions share one exactly-once bookmark.
func DefaultGroup(db, table string) string {
	return "mo_kafka_" + db + "_" + table
}

// BuildCreateSQLEnvelope renders the config into the planner-owned
// rel_createsql comment envelope.
func BuildCreateSQLEnvelope(cfg Config) string {
	return fmt.Sprintf(
		"/* %s version=1; kind=%s; brokers=%s; topic=%s; partition=%d; autocommit=%t; group=%s; format=%s; separator=%s */",
		CreateSQLEnvelopePrefix,
		CreateSQLKindKafka,
		url.QueryEscape(cfg.Brokers),
		url.QueryEscape(cfg.Topic),
		cfg.Partition,
		cfg.Autocommit,
		url.QueryEscape(cfg.Group),
		cfg.Format,
		url.QueryEscape(cfg.Separator),
	)
}

// ParseCreateSQLEnvelope recognizes and parses the Kafka envelope. The bool
// result reports whether the string is a Kafka envelope at all; only the
// anchored leading comment is recognized (rel_createsql of a generic external
// table is user-controlled JSON — same injection rationale as the datastream
// and MongoDB envelopes).
func ParseCreateSQLEnvelope(ctx context.Context, createSQL string) (Config, bool, error) {
	createSQL = strings.TrimSpace(createSQL)
	prefix := "/* " + CreateSQLEnvelopePrefix
	if !strings.HasPrefix(createSQL, prefix) {
		return Config{}, false, nil
	}
	start := len(prefix)
	end := strings.Index(createSQL[start:], "*/")
	if end < 0 {
		return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope is not closed")
	}
	fields := make(map[string]string)
	for _, part := range strings.Split(createSQL[start:start+end], ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope field must be key=value")
		}
		decoded, err := url.QueryUnescape(strings.TrimSpace(value))
		if err != nil {
			return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope field is not url-escaped")
		}
		fields[strings.ToLower(strings.TrimSpace(key))] = decoded
	}
	if version, err := strconv.Atoi(fields["version"]); err != nil || version != 1 {
		return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope version must be 1")
	}
	if fields["kind"] != CreateSQLKindKafka {
		return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope kind must be kafka_table")
	}
	partition, err := strconv.ParseInt(fields["partition"], 10, 32)
	if err != nil || partition < 0 {
		return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope has an invalid partition")
	}
	autocommit, err := strconv.ParseBool(fields["autocommit"])
	if err != nil {
		return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope has an invalid autocommit flag")
	}
	cfg := Config{
		Brokers:    fields["brokers"],
		Topic:      fields["topic"],
		Partition:  int32(partition),
		Autocommit: autocommit,
		Group:      fields["group"],
		Format:     fields["format"],
		Separator:  fields["separator"],
	}
	if cfg.Brokers == "" || cfg.Topic == "" || cfg.Group == "" {
		return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope missing brokers, topic, or group")
	}
	if cfg.Format != FormatCSV && cfg.Format != FormatJSONL {
		return Config{}, true, moerr.NewInvalidInput(ctx, "kafka rel_createsql envelope has an invalid format")
	}
	if cfg.Separator == "" {
		cfg.Separator = ","
	}
	return cfg, true, nil
}
