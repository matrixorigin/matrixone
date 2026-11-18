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
	"context"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	ScopedChangedTablesMaxTables = 100_000

	scopedChangedTablesVersion    = 1
	scopedChangedTablesComplete   = 1
	scopedChangedTablesHeaderSize = 30 // version, flags, two 12-byte timestamps, count
	scopedChangedTableWireSize    = 20 // uint32 account, uint64 database, uint64 table
)

type ScopedChangedTable struct {
	AccountID  uint32
	DatabaseID uint64
	TableID    uint64
}

type GetScopedChangedTablesReq struct {
	From, To timestamp.Timestamp  // (From, To]
	Tables   []ScopedChangedTable // Empty scope checks capability without scanning history.
}

type GetScopedChangedTablesResp struct {
	From, To timestamp.Timestamp
	Complete bool
	Tables   []ScopedChangedTable
}

func validScopedChangedTablesWindow(from, to timestamp.Timestamp) bool {
	return from.NodeID == 0 && to.NodeID == 0 && from.PhysicalTime >= 0 && !from.Greater(to)
}

func (req *GetScopedChangedTablesReq) AllowedTables(ctx context.Context) (map[ScopedChangedTable]struct{}, error) {
	if req == nil || !validScopedChangedTablesWindow(req.From, req.To) || len(req.Tables) > ScopedChangedTablesMaxTables {
		return nil, moerr.NewInvalidInput(ctx, "invalid scoped changed-table request")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tables := make(map[ScopedChangedTable]struct{}, len(req.Tables))
	for _, table := range req.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if table.DatabaseID == 0 || table.TableID == 0 {
			return nil, moerr.NewInvalidInput(ctx, "invalid scoped changed-table identity")
		}
		if _, ok := tables[table]; ok {
			return nil, moerr.NewInvalidInput(ctx, "duplicate scoped changed-table identity")
		}
		tables[table] = struct{}{}
	}
	return tables, nil
}

// ValidateScopedChangedTablesResponse verifies the completion proof before the
// caller can publish a marker or expose table changes.
func ValidateScopedChangedTablesResponse(ctx context.Context, req *GetScopedChangedTablesReq, resp *GetScopedChangedTablesResp) error {
	allowed, err := req.AllowedTables(ctx)
	if err != nil {
		return err
	}
	if resp == nil || !resp.Complete || !validScopedChangedTablesWindow(resp.From, resp.To) ||
		!resp.From.Equal(req.From) || !resp.To.Equal(req.To) || len(resp.Tables) > len(allowed) ||
		(req.From.Equal(req.To) && len(resp.Tables) != 0) {
		return moerr.NewInvalidInput(ctx, "incomplete scoped changed-table response or mismatched window")
	}
	for _, table := range resp.Tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := allowed[table]; !ok {
			return moerr.NewInvalidInput(ctx, "scoped changed-table response contains a duplicate or unrequested identity")
		}
		delete(allowed, table)
	}
	return ctx.Err()
}

func (req *GetScopedChangedTablesReq) MarshalBinary() ([]byte, error) {
	return marshalScopedChangedTables(req.From, req.To, 0, req.Tables)
}

func (req *GetScopedChangedTablesReq) UnmarshalBinary(data []byte) error {
	from, to, _, tables, err := unmarshalScopedChangedTables(data, false)
	if err == nil {
		*req = GetScopedChangedTablesReq{From: from, To: to, Tables: tables}
	}
	return err
}

func (resp *GetScopedChangedTablesResp) MarshalBinary() ([]byte, error) {
	var flags byte
	if resp.Complete {
		flags = scopedChangedTablesComplete
	}
	return marshalScopedChangedTables(resp.From, resp.To, flags, resp.Tables)
}

func (resp *GetScopedChangedTablesResp) UnmarshalBinary(data []byte) error {
	from, to, complete, tables, err := unmarshalScopedChangedTables(data, true)
	if err == nil {
		*resp = GetScopedChangedTablesResp{From: from, To: to, Complete: complete, Tables: tables}
	}
	return err
}

func marshalScopedChangedTables(from, to timestamp.Timestamp, flags byte, tables []ScopedChangedTable) ([]byte, error) {
	if !validScopedChangedTablesWindow(from, to) || len(tables) > ScopedChangedTablesMaxTables {
		return nil, moerr.NewInvalidInputNoCtx("invalid scoped changed-table window or table count")
	}
	data := make([]byte, scopedChangedTablesHeaderSize+len(tables)*scopedChangedTableWireSize)
	data[0], data[1] = scopedChangedTablesVersion, flags
	encodeScopedChangedTablesTimestamp(data[2:14], from)
	encodeScopedChangedTablesTimestamp(data[14:26], to)
	binary.LittleEndian.PutUint32(data[26:30], uint32(len(tables)))
	for i, table := range tables {
		start := scopedChangedTablesHeaderSize + i*scopedChangedTableWireSize
		tuple := data[start : start+scopedChangedTableWireSize]
		binary.LittleEndian.PutUint32(tuple[:4], table.AccountID)
		binary.LittleEndian.PutUint64(tuple[4:12], table.DatabaseID)
		binary.LittleEndian.PutUint64(tuple[12:20], table.TableID)
	}
	return data, nil
}

func unmarshalScopedChangedTables(data []byte, response bool) (from, to timestamp.Timestamp, complete bool, tables []ScopedChangedTable, err error) {
	if len(data) < scopedChangedTablesHeaderSize || data[0] != scopedChangedTablesVersion ||
		(!response && data[1] != 0) || (response && data[1] > scopedChangedTablesComplete) {
		err = moerr.NewInvalidInputNoCtx("invalid scoped changed-table header")
		return
	}
	count := binary.LittleEndian.Uint32(data[26:30])
	if count > ScopedChangedTablesMaxTables || len(data) != scopedChangedTablesHeaderSize+int(count)*scopedChangedTableWireSize {
		err = moerr.NewInvalidInputNoCtx("invalid scoped changed-table payload size")
		return
	}
	from = decodeScopedChangedTablesTimestamp(data[2:14])
	to = decodeScopedChangedTablesTimestamp(data[14:26])
	if !validScopedChangedTablesWindow(from, to) {
		err = moerr.NewInvalidInputNoCtx("invalid scoped changed-table window")
		return
	}
	complete = data[1] == scopedChangedTablesComplete
	if count != 0 {
		tables = make([]ScopedChangedTable, count)
	}
	for i := range tables {
		start := scopedChangedTablesHeaderSize + i*scopedChangedTableWireSize
		tuple := data[start : start+scopedChangedTableWireSize]
		tables[i] = ScopedChangedTable{
			AccountID:  binary.LittleEndian.Uint32(tuple[:4]),
			DatabaseID: binary.LittleEndian.Uint64(tuple[4:12]),
			TableID:    binary.LittleEndian.Uint64(tuple[12:20]),
		}
	}
	return
}

func encodeScopedChangedTablesTimestamp(data []byte, ts timestamp.Timestamp) {
	binary.LittleEndian.PutUint64(data[:8], uint64(ts.PhysicalTime))
	binary.LittleEndian.PutUint32(data[8:12], ts.LogicalTime)
}

func decodeScopedChangedTablesTimestamp(data []byte) timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: int64(binary.LittleEndian.Uint64(data[:8])),
		LogicalTime:  binary.LittleEndian.Uint32(data[8:12]),
	}
}
