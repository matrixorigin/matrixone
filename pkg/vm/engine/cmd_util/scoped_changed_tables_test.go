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

package cmd_util

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestScopedChangedTablesWireLimit(t *testing.T) {
	req := GetScopedChangedTablesReq{
		From:   timestamp.Timestamp{PhysicalTime: 12, LogicalTime: 17},
		To:     timestamp.Timestamp{PhysicalTime: 12, LogicalTime: 18},
		Tables: make([]ScopedChangedTable, ScopedChangedTablesMaxTables),
	}
	for i := range req.Tables {
		req.Tables[i] = ScopedChangedTable{AccountID: uint32(i / 20), DatabaseID: uint64(i/20 + 1), TableID: uint64(i + 1)}
	}
	wire, err := req.MarshalBinary()
	require.NoError(t, err)
	require.Len(t, wire, 2_000_030)
	var got GetScopedChangedTablesReq
	require.NoError(t, got.UnmarshalBinary(wire))
	require.Equal(t, req, got)

	response := GetScopedChangedTablesResp{From: req.From, To: req.To, Complete: true, Tables: req.Tables}
	wire, err = response.MarshalBinary()
	require.NoError(t, err)
	var gotResponse GetScopedChangedTablesResp
	require.NoError(t, gotResponse.UnmarshalBinary(wire))
	require.Equal(t, response, gotResponse)
	require.NoError(t, ValidateScopedChangedTablesResponse(t.Context(), &req, &gotResponse))
	req.Tables = append(req.Tables, ScopedChangedTable{})
	_, err = req.MarshalBinary()
	require.Error(t, err)
}

func TestScopedChangedTablesWireRejectsMalformedPayload(t *testing.T) {
	req := GetScopedChangedTablesReq{
		From:   timestamp.Timestamp{PhysicalTime: 2},
		To:     timestamp.Timestamp{PhysicalTime: 3},
		Tables: []ScopedChangedTable{{DatabaseID: 5, TableID: 8}},
	}
	wire, err := req.MarshalBinary()
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"empty", func([]byte) []byte { return nil }},
		{"truncated", func(data []byte) []byte { return data[:len(data)-1] }},
		{"trailing", func(data []byte) []byte { return append(data, 0) }},
		{"version", func(data []byte) []byte { data[0]++; return data }},
		{"request flags", func(data []byte) []byte { data[1] = 1; return data }},
		{"oversized advertised count", func(data []byte) []byte {
			binary.LittleEndian.PutUint32(data[26:30], ScopedChangedTablesMaxTables+1)
			return data
		}},
		{"inverted window", func(data []byte) []byte {
			binary.LittleEndian.PutUint64(data[14:22], 1)
			return data
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got GetScopedChangedTablesReq
			require.Error(t, got.UnmarshalBinary(tc.mutate(bytes.Clone(wire))))
			require.Empty(t, got.Tables)
		})
	}
	wire[1] = 2
	require.Error(t, (&GetScopedChangedTablesResp{}).UnmarshalBinary(wire))
}

func TestScopedChangedTablesValidatesCompletionAndScope(t *testing.T) {
	from := timestamp.Timestamp{PhysicalTime: 12, LogicalTime: 3}
	to := from.Next()
	allowed := ScopedChangedTable{AccountID: 1, DatabaseID: 2, TableID: 3}
	other := ScopedChangedTable{AccountID: 1, DatabaseID: 2, TableID: 4}
	req := GetScopedChangedTablesReq{From: from, To: to, Tables: []ScopedChangedTable{allowed, other}}
	response := GetScopedChangedTablesResp{From: from, To: to, Complete: true, Tables: []ScopedChangedTable{allowed}}
	require.NoError(t, ValidateScopedChangedTablesResponse(t.Context(), &req, &response))
	for _, tc := range []struct {
		name   string
		mutate func(*GetScopedChangedTablesResp)
	}{
		{"incomplete", func(r *GetScopedChangedTablesResp) { r.Complete = false }},
		{"wrong lower endpoint", func(r *GetScopedChangedTablesResp) { r.From = to }},
		{"wrong upper endpoint", func(r *GetScopedChangedTablesResp) { r.To = to.Next() }},
		{"wrong account", func(r *GetScopedChangedTablesResp) { r.Tables[0].AccountID++ }},
		{"wrong database", func(r *GetScopedChangedTablesResp) { r.Tables[0].DatabaseID++ }},
		{"wrong table", func(r *GetScopedChangedTablesResp) { r.Tables[0].TableID += 2 }},
		{"duplicate", func(r *GetScopedChangedTablesResp) { r.Tables = append(r.Tables, allowed) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := response
			got.Tables = []ScopedChangedTable{allowed}
			tc.mutate(&got)
			require.Error(t, ValidateScopedChangedTablesResponse(t.Context(), &req, &got))
		})
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, ValidateScopedChangedTablesResponse(canceled, &req, &response), context.Canceled)

	probe := GetScopedChangedTablesReq{From: to, To: to}
	ack := GetScopedChangedTablesResp{From: to, To: to, Complete: true}
	require.NoError(t, ValidateScopedChangedTablesResponse(t.Context(), &probe, &ack))
	probe.Tables = []ScopedChangedTable{allowed}
	ack.Tables = []ScopedChangedTable{allowed}
	require.Error(t, ValidateScopedChangedTablesResponse(t.Context(), &probe, &ack))

	req.Tables = []ScopedChangedTable{allowed, allowed}
	_, err := req.AllowedTables(t.Context())
	require.ErrorContains(t, err, "duplicate")
	req.Tables = []ScopedChangedTable{{DatabaseID: 2}}
	_, err = req.AllowedTables(t.Context())
	require.ErrorContains(t, err, "invalid")
}
