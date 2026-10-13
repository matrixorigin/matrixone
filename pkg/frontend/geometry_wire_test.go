// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/stretchr/testify/require"
	"testing"
)

func cleanupGeometryWireFixture(t *testing.T, proto *MysqlProtocolImpl, stmt *PrepareStmt) {
	t.Helper()
	ioConn := proto.GetTcpConnection()
	allocator := ioConn.allocator.allocator

	// Cleanup runs in reverse registration order, before the process cleanup.
	t.Cleanup(func() {
		alloc, ok := allocator.(*LeakCheckAllocator)
		require.True(t, ok, "expected fixture leak-check allocator")
		alloc.Lock()
		allocated, freed, records := alloc.allocated, alloc.freed, len(alloc.records)
		alloc.Unlock()
		require.Positive(t, allocated, "fixture must exercise allocation")
		require.Equal(t, allocated, freed, "unreleased IO buffers")
		require.Zero(t, records, "live IO allocations")
	})
	t.Cleanup(func() {
		if err := ioConn.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(proto.Close)
	t.Cleanup(stmt.Close)
}

func TestGeometryWireWriters(t *testing.T) {
	// Independent standard WKB for POINT(1 2), not the production codec's output.
	point, _ := hex.DecodeString("0101000000000000000000f03f0000000000000040")
	for _, oid := range []types.T{types.T_geometry, types.T_geometry32} {
		for _, srid := range []int32{0, 4326} {
			for _, fast := range []bool{false, true} {
				for _, binaryRow := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/srid=%d/vector=%v/binary=%v", oid, srid, fast, binaryRow), func(t *testing.T) {
						conn := &prepareResponseCaptureConn{}
						proto, proc, stmt := newBinaryPrepareProtocolTestCaseWithConn(t, "select 1", conn)
						cleanupGeometryWireFixture(t, proto, stmt)
						typ := oid.ToType()
						typ.Width = srid + 1
						col := new(MysqlColumn)
						col.SetName("g")
						require.NoError(t, setMysqlColumnTypeInfo(context.Background(), typ, col))
						require.Equal(t, defines.MYSQL_TYPE_GEOMETRY, col.ColumnType())
						require.Equal(t, uint16(63), col.Charset())
						require.NotZero(t, col.Flag()&uint16(defines.BINARY_FLAG))
						definition := parsePrepareColumnDefinition(t, proto.makeColumnDefinition41Payload(col, int(COM_QUERY))[HeaderOffset:])
						require.Equal(t, defines.MYSQL_TYPE_GEOMETRY, definition.typ)
						require.Equal(t, uint16(63), definition.charset)
						require.NotZero(t, definition.flags&uint16(defines.BINARY_FLAG))
						mrs := &MysqlResultSet{}
						mrs.AddColumn(col)
						mrs.AddRow([]any{[]byte("POINT(1 2)")})
						if fast {
							bat := batch.NewWithSize(1)
							defer bat.Clean(proc.Mp())
							bat.Vecs[0] = vector.NewVec(typ)
							payload := point
							if oid == types.T_geometry32 {
								var err error
								payload, err = geo.WriteWKBFloat32(geo.Point{X: 1, Y: 2})
								require.NoError(t, err)
							}
							require.NoError(t, vector.AppendBytes(bat.Vecs[0], payload, false, proc.Mp()))
							bat.SetRowCount(1)
							slices := &ColumnSlices{ctx: context.Background(), colIdx2SliceIdx: make([]int, 1), dataSet: bat}
							defer slices.Close()
							require.NoError(t, convertBatchToSlices(context.Background(), proto.GetSession(), bat, slices))
							if binaryRow {
								require.NoError(t, proto.appendResultSetBinaryRow2(mrs, slices, 0))
							} else {
								require.NoError(t, proto.appendResultSetTextRow2(mrs, slices, 0))
							}
							// Internal execution and user variables retain their readable contract.
							row := make([]any, 1)
							require.NoError(t, extractRowFromEveryVector(context.Background(), proto.GetSession(), bat, 0, row, false))
							require.Equal(t, []byte("POINT(1 2)"), row[0])
						} else if binaryRow {
							require.NoError(t, proto.appendResultSetBinaryRow(mrs, 0))
						} else {
							require.NoError(t, proto.appendResultSetTextRow(mrs, 0))
						}
						require.NoError(t, proto.flush())
						packets := splitProtocolPackets(t, conn.writes)
						require.Len(t, packets, 1)
						expected := append([]byte{0, 0, 0, 0}, point...)
						if srid == 4326 {
							expected[0], expected[1] = 0xe6, 0x10
						}
						row := append([]byte{byte(len(expected))}, expected...)
						if binaryRow {
							row = append([]byte{0, 0}, row...)
						}
						require.Equal(t, row, packets[0])
					})
				}
			}
		}
	}
}

func TestGeometryWireConversionBoundaries(t *testing.T) {
	for _, oid := range []types.T{types.T_geometry, types.T_geometry32} {
		typ := oid.ToType()
		typ.Width = 4327
		for _, text := range []string{"POINT EMPTY", "LINESTRING(0 0,1 1)", "POLYGON((0 0,1 0,0 1,0 0))", "MULTIPOINT(EMPTY,1 2)", "MULTILINESTRING((0 0,1 1))", "MULTIPOLYGON(EMPTY,((0 0,1 0,0 1,0 0)))", "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY)"} {
			g, err := geo.ParseWKT(text)
			require.NoError(t, err)
			payload := geo.WriteWKB(g)
			if oid == types.T_geometry32 {
				payload, err = geo.WriteWKBFloat32(g)
				require.NoError(t, err)
			}
			wire, err := encodeMySQLGeometry(payload, typ)
			require.NoError(t, err, text)
			require.Equal(t, []byte{0xe6, 0x10, 0, 0}, wire[:4])
			decoded, err := geo.ReadWKB(wire[4:])
			require.NoError(t, err)
			require.Equal(t, geo.WriteWKT(g), geo.WriteWKT(decoded))
			payload[0] = 9
			require.Equal(t, byte(1), wire[4], "result must own its bytes")
		}
		_, err := encodeMySQLGeometry([]byte{1, 1}, typ)
		require.Error(t, err)
	}
	_, err := encodeMySQLGeometry([]byte("POINT(1 2)"), types.Type{})
	require.ErrorContains(t, err, "missing geometry source type")
}

func TestHistoricalGeometryCursorCaptureAndFetch(t *testing.T) {
	// Independent little-endian WKB for the four-position historical ring.
	historical, err := hex.DecodeString("01030000000100000004000000" +
		"00000000000000000000000000000000" +
		"000000000000f03f0000000000000000" +
		"000000000000f03f000000000000f03f" +
		"0000000000000000000000000000f03f")
	require.NoError(t, err)
	closed := append([]byte(nil), historical...)
	closed[9] = 5
	closed = append(closed, make([]byte, 16)...)
	for _, oid := range []types.T{types.T_geometry, types.T_geometry32} {
		for _, srid := range []int32{0, 4326} {
			t.Run(fmt.Sprintf("%s/srid=%d", oid, srid), func(t *testing.T) {
				ctx := context.Background()
				conn := &prepareResponseCaptureConn{}
				proto, proc, stmt := newBinaryPrepareProtocolTestCaseWithConn(t, "select 1", conn)
				cleanupGeometryWireFixture(t, proto, stmt)
				ses := proto.GetSession()
				ses.proc = proc
				ses.respr = NewMysqlResp(proto)
				ses.SetCmd(COM_STMT_FETCH)
				ses.prepareStmts = map[string]*PrepareStmt{stmt.Name: stmt}
				typ := oid.ToType()
				typ.Width = srid + 1
				col := new(MysqlColumn)
				require.NoError(t, setMysqlColumnTypeInfo(ctx, typ, col))
				stmt.cursor = &preparedStmtCursor{result: &MysqlResultSet{Columns: []Column{col}}, maxRows: 3, maxBytes: 4096}
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(typ)
				defer bat.Clean(proc.Mp())
				for _, standard := range [][]byte{historical, closed} {
					payload := standard
					if oid == types.T_geometry32 {
						g, err := geo.ReadWKB(standard)
						require.NoError(t, err)
						payload, err = geo.WriteWKBFloat32(g)
						require.NoError(t, err)
					}
					require.NoError(t, vector.AppendBytes(bat.Vecs[0], payload, false, proc.Mp()))
				}
				require.NoError(t, vector.AppendBytes(bat.Vecs[0], nil, true, proc.Mp()))
				bat.SetRowCount(3)
				require.NoError(t, capturePreparedCursorBatch(ses, &ExecCtx{reqCtx: ctx, prepareStmt: stmt}, bat))
				require.Equal(t, []byte("POLYGON((0 0,1 0,1 1,0 1))"), stmt.cursor.result.Data[0][0])
				require.Positive(t, ses.preparedCursorBytes.Load())
				// Retained WKT owns its bytes independently of the input vectors.
				bat.Vecs[0].GetBytesAt(0)[0] = 9
				request := make([]byte, 8)
				binary.LittleEndian.PutUint32(request[:4], 1)
				binary.LittleEndian.PutUint32(request[4:], 2)
				response, err := executeStmtFetch(ctx, ses, request)
				require.NoError(t, err)
				require.Nil(t, response)
				require.NotNil(t, stmt.cursor)
				require.Equal(t, uint64(2), stmt.cursor.offset)
				require.NoError(t, proto.flush())
				packets := splitProtocolPackets(t, conn.writes)
				require.Len(t, packets, 3)
				for i, wkb := range [][]byte{historical, closed} {
					prefix := []byte{0, 0, 0, 0}
					binary.LittleEndian.PutUint32(prefix, uint32(srid))
					wire := append(prefix, wkb...)
					want := append([]byte{0, 0, byte(len(wire))}, wire...)
					require.Equal(t, want, packets[i])
				}
				require.Equal(t, byte(defines.EOFHeader), packets[2][0])
				require.NotZero(t, binary.LittleEndian.Uint16(packets[2][3:5])&SERVER_STATUS_CURSOR_EXISTS)
				conn.writes = nil
				response, err = executeStmtFetch(ctx, ses, request)
				require.NoError(t, err)
				require.Nil(t, response)
				require.NoError(t, proto.flush())
				packets = splitProtocolPackets(t, conn.writes)
				require.Len(t, packets, 2)
				require.Equal(t, []byte{0, 4}, packets[0])
				require.Equal(t, byte(defines.EOFHeader), packets[1][0])
				status := binary.LittleEndian.Uint16(packets[1][3:5])
				require.NotZero(t, status&SERVER_STATUS_LAST_ROW_SENT)
				require.Zero(t, status&SERVER_STATUS_CURSOR_EXISTS)
				require.Nil(t, stmt.cursor)
				require.Zero(t, ses.preparedCursorBytes.Load())
			})
		}
	}
}
