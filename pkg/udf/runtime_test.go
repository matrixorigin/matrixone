// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package udf

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type closableRuntime struct {
	language string
	closed   atomic.Int32
}

func (r *closableRuntime) Language() string { return r.language }

func (r *closableRuntime) Execute(
	context.Context,
	*Invocation,
	vector.FunctionResultWrapper,
	*mpool.MPool,
) error {
	return nil
}

func (r *closableRuntime) Close() error {
	r.closed.Add(1)
	return nil
}

func TestRuntimeRegistryClosesOwnedRuntimesOnce(t *testing.T) {
	python := &closableRuntime{language: LanguagePython}
	runtime, err := NewRuntime(python)
	require.NoError(t, err)
	closer, ok := runtime.(RuntimeCloser)
	require.True(t, ok)
	require.NoError(t, closer.Close())
	require.NoError(t, closer.Close())
	require.Equal(t, int32(1), python.closed.Load())
}

func TestStatementContextFromMapProducesCanonicalTypedSnapshot(t *testing.T) {
	snapshot, err := StatementContextFromMap(map[string]string{
		"statement_timestamp_utc":         "1704067200123456",
		"session_timezone_kind":           "FIXED_OFFSET",
		"session_timezone_offset_minutes": "+510",
		"sql_mode":                        "[\"ANSI\",\"STRICT_TRANS_TABLES\"]",
		"current_database":                "app",
		"current_user":                    "alice",
		"current_role":                    "writer",
		"connection_collation":            "utf8mb4_bin",
	})
	require.NoError(t, err)
	require.Equal(t, int32(510), snapshot.TimezoneOffsetMinutes)
	require.Equal(t, []string{"ANSI", "STRICT_TRANS_TABLES"}, snapshot.SQLMode)
	require.NoError(t, snapshot.Validate())

	snapshot.SQLMode = []string{"STRICT_TRANS_TABLES", "ANSI"}
	require.ErrorContains(t, snapshot.Validate(), "not sorted")
}

func TestStatementContextFromMapRejectsAmbiguousTimezoneAndMalformedMode(t *testing.T) {
	base := map[string]string{
		"statement_timestamp_utc":         "1704067200123456",
		"session_timezone_kind":           "FIXED_OFFSET",
		"session_timezone_offset_minutes": "+480",
		"sql_mode":                        "[]",
		"current_user":                    "alice",
		"connection_collation":            "utf8mb4_bin",
	}
	ambiguous := make(map[string]string, len(base)+1)
	for key, value := range base {
		ambiguous[key] = value
	}
	ambiguous["session_timezone_name"] = "Asia/Shanghai"
	_, err := StatementContextFromMap(ambiguous)
	require.ErrorContains(t, err, "fixed statement timezone")

	malformed := make(map[string]string, len(base))
	for key, value := range base {
		malformed[key] = value
	}
	malformed["sql_mode"] = "[\"ANSI\",\"ANSI\"]"
	_, err = StatementContextFromMap(malformed)
	require.ErrorContains(t, err, "not canonical")

	malformed["sql_mode"] = "null"
	_, err = StatementContextFromMap(malformed)
	require.ErrorContains(t, err, "expected a JSON array")
}

func TestStatementContextAcceptsUnixEpoch(t *testing.T) {
	snapshot, err := StatementContextFromMap(map[string]string{
		"statement_timestamp_utc":         "0",
		"session_timezone_kind":           "FIXED_OFFSET",
		"session_timezone_offset_minutes": "0",
		"sql_mode":                        "[]",
		"current_user":                    "alice",
		"connection_collation":            "utf8mb4_bin",
	})
	require.NoError(t, err)
	require.Zero(t, snapshot.StatementTimestampUTC)
	require.NoError(t, snapshot.Validate())
}

func TestStatementContextRejectsUnknownIANAZoneBeforeTransport(t *testing.T) {
	snapshot := StatementContext{
		ContractVersion:         StatementContextContractVersion,
		StatementTimestampUTC:   0,
		TimezoneKind:            "IANA",
		TimezoneName:            "NoSuch/Zone",
		TimezoneDatabaseVersion: "2026a",
		CurrentUser:             "alice",
		ConnectionCollation:     "utf8mb4_bin",
	}
	require.ErrorContains(t, snapshot.Validate(), "not present in the local tzdb")
}

func TestStatementContextAcceptsKnownIANAZone(t *testing.T) {
	version, err := TimezoneDatabaseVersion()
	require.NoError(t, err)
	snapshot := StatementContext{
		ContractVersion:         StatementContextContractVersion,
		StatementTimestampUTC:   0,
		TimezoneKind:            "IANA",
		TimezoneName:            "UTC",
		TimezoneDatabaseVersion: version,
		CurrentUser:             "alice",
		ConnectionCollation:     "utf8mb4_bin",
	}
	require.NoError(t, snapshot.Validate())
}

func TestStatementContextMatchesPythonDatetimeRange(t *testing.T) {
	base := StatementContext{
		ContractVersion:     StatementContextContractVersion,
		TimezoneKind:        "FIXED_OFFSET",
		CurrentUser:         "alice",
		ConnectionCollation: "utf8mb4_bin",
	}
	for _, timestamp := range []int64{minStatementTimestampUTC - 1, maxStatementTimestampUTC + 1} {
		context := base
		context.StatementTimestampUTC = timestamp
		require.ErrorContains(t, context.Validate(), "outside the Python datetime range")
	}
}

func TestSecurityFrameRejectsEffectivePrincipalChange(t *testing.T) {
	frame := SecurityFrame{
		ContractVersion: SecurityFrameContractVersion,
		Mode:            "INVOKER",
		InvokerUserID:   7,
		EffectiveUserID: 8,
	}
	require.ErrorContains(t, frame.Validate(), "changed effective principal")
}
