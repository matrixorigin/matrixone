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

package external

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/foreigntvf"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// fakeScanSession implements process.Session + process.ForeignConnCache.
type fakeScanSession struct {
	conns map[string]process.ForeignConn
}

func (s *fakeScanSession) GetTempTable(dbName, alias string) (string, bool) { return "", false }
func (s *fakeScanSession) AddTempTable(dbName, alias, realName string)      {}
func (s *fakeScanSession) RemoveTempTable(dbName, alias string)             {}
func (s *fakeScanSession) RemoveTempTableByRealName(realName string)        {}
func (s *fakeScanSession) GetSqlModeNoAutoValueOnZero() (bool, bool)        { return false, false }
func (s *fakeScanSession) PutForeignConn(_ context.Context, handle string, c process.ForeignConn) (process.ForeignConn, error) {
	if s.conns == nil {
		s.conns = make(map[string]process.ForeignConn)
	}
	if existing, ok := s.conns[handle]; ok && existing != nil {
		return existing, nil
	}
	s.conns[handle] = c
	return c, nil
}
func (s *fakeScanSession) GetForeignConn(handle string) (process.ForeignConn, bool) {
	c, ok := s.conns[handle]
	return c, ok
}
func (s *fakeScanSession) RemoveForeignConn(handle string) (process.ForeignConn, bool) {
	c, ok := s.conns[handle]
	if ok {
		delete(s.conns, handle)
	}
	return c, ok
}

// fakeScanConn replays a fixed CSV stream and records the query it got.
type fakeScanConn struct {
	kind      foreigntvf.Kind
	csv       string
	lastQuery string
}

func (c *fakeScanConn) Close() error          { return nil }
func (c *fakeScanConn) Kind() foreigntvf.Kind { return c.kind }
func (c *fakeScanConn) Query(ctx context.Context, q string) (io.ReadCloser, error) {
	c.lastQuery = q
	return io.NopCloser(strings.NewReader(c.csv)), nil
}

func foreignScanParam(t *testing.T, kind string, cfg string, cols []*plan.ColDef, names []string) *ExternalParam {
	param := &ExternalParam{}
	param.Extern = ForeignExternParam(kind)
	param.ForeignScan = &plan.ForeignScan{Kind: kind, Config: cfg}
	param.Cols = cols
	attrs := make([]plan.ExternAttr, len(cols))
	for i, col := range cols {
		attrs[i] = plan.ExternAttr{ColName: col.Name, ColIndex: int32(i), ColFieldIndex: int32(i)}
	}
	param.Attrs = attrs
	param.ColumnListLen = int32(len(names))
	param.Fileparam = &ExFileparam{}
	param.maxBatchSize = 1 << 20
	// mirror External.Prepare's flag derivation
	param.ESQLTemporalUTC = kind == "esql"
	return param
}

func foreignScanCols() []*plan.ColDef {
	mk := func(name string, tt types.T) *plan.ColDef {
		typ := tt.ToType()
		return &plan.ColDef{Name: name, Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale}}
	}
	return []*plan.ColDef{mk("id", types.T_int64), mk("name", types.T_varchar)}
}

// TestForeignScanReaderOpenReadClose drives the full reader lifecycle with a
// seeded session connection: Open resolves the conn, sends the current
// Fileparam.Filepath as the query, ReadBatch materializes typed rows, Close is
// idempotent, and a second Open (next "file") re-arms the parser.
func TestForeignScanReaderOpenReadClose(t *testing.T) {
	proc := testutil.NewProcess(t)
	ses := &fakeScanSession{}
	proc.Session = ses
	cfg := `{"driver":"nope","dsn":"unused"}` // never dialed: conn pre-seeded
	conn := &fakeScanConn{kind: foreigntvf.KindSQL, csv: "\"1\",\"alice\"\n\"2\",\\N\n"}
	ses.PutForeignConn(context.TODO(), foreigntvf.MakeHandle(foreigntvf.KindSQL, cfg), conn)

	param := foreignScanParam(t, "sql", cfg, foreignScanCols(), []string{"id", "name"})
	param.Fileparam.Filepath = "select q1"

	r := NewForeignScanReader(param)
	empty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	require.Equal(t, "select q1", conn.lastQuery)

	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"id", "name"}
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	defer bat.Clean(proc.Mp())

	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, "alice", bat.Vecs[1].GetStringAt(0))
	require.True(t, bat.Vecs[1].GetNulls().Contains(1))

	require.NoError(t, r.Close())
	require.NoError(t, r.Close()) // idempotent

	// next "file": Open again with a new query text
	conn.csv = "\"3\",\"carol\"\n"
	param.Fileparam.Filepath = "select q2"
	_, err = r.Open(param, proc)
	require.NoError(t, err)
	require.Equal(t, "select q2", conn.lastQuery)
	bat.CleanOnlyData()
	finished, err = r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 1, bat.RowCount())
	require.NoError(t, r.Close())
}

// TestForeignScanReaderESQLHeaderSkip proves the ESQL dialect skips one header
// line per opened query and re-arms the skip on the next Open.
func TestForeignScanReaderESQLHeaderSkip(t *testing.T) {
	proc := testutil.NewProcess(t)
	ses := &fakeScanSession{}
	proc.Session = ses
	cfg := `{"addresses":["http://unused"]}`
	conn := &fakeScanConn{kind: foreigntvf.KindESQL, csv: "id,name\r\n1,alice\r\n"}
	ses.PutForeignConn(context.TODO(), foreigntvf.MakeHandle(foreigntvf.KindESQL, cfg), conn)

	param := foreignScanParam(t, "esql", cfg, foreignScanCols(), []string{"id", "name"})
	require.Equal(t, uint64(1), param.Extern.Tail.IgnoredLines)
	param.Fileparam.Filepath = "FROM idx"

	r := NewForeignScanReader(param)
	for i := 0; i < 2; i++ { // twice: header skip must re-arm per Open
		conn.csv = "id,name\r\n7,bob\r\n"
		_, err := r.Open(param, proc)
		require.NoError(t, err)
		bat := batch.NewWithSize(2)
		bat.Attrs = []string{"id", "name"}
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
		require.NoError(t, err)
		require.True(t, finished)
		require.Equal(t, 1, bat.RowCount())
		require.Equal(t, "bob", bat.Vecs[1].GetStringAt(0))
		bat.Clean(proc.Mp())
		require.NoError(t, r.Close())
	}
}

// TestForeignScanReaderErrors covers the unhappy Open paths: missing scan
// metadata, a session without the cache capability, a malformed inline
// config, and no config anywhere.
func TestForeignScanReaderErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	cols := foreignScanCols()

	// missing metadata
	param := foreignScanParam(t, "sql", "", cols, []string{"id", "name"})
	param.ForeignScan = nil
	r := NewForeignScanReader(param)
	_, err := r.Open(param, proc)
	require.ErrorContains(t, err, "without scan metadata")

	// no interactive session
	proc.Session = nil
	param = foreignScanParam(t, "sql", "x", cols, []string{"id", "name"})
	_, err = NewForeignScanReader(param).Open(param, proc)
	require.ErrorContains(t, err, "interactive session")

	// a malformed inline config fails cleanly at connect
	proc.Session = &fakeScanSession{}
	param = foreignScanParam(t, "sql", "not-json", cols, []string{"id", "name"})
	_, err = NewForeignScanReader(param).Open(param, proc)
	require.ErrorContains(t, err, "invalid config")

	// no config and no session variable
	param = foreignScanParam(t, "sql", "", cols, []string{"id", "name"})
	_, err = NewForeignScanReader(param).Open(param, proc)
	require.Error(t, err)
}

// TestForeignExternParamDialects pins the two dialects: SQL uses the plain
// MySQL CSV defaults; ESQL disables backslash escaping and skips one header.
func TestForeignExternParamDialects(t *testing.T) {
	sqlParam := ForeignExternParam("sql")
	require.Equal(t, tree.INFILE, sqlParam.ScanType)
	require.Equal(t, tree.CSV, sqlParam.Format)
	require.Nil(t, sqlParam.Tail.Fields)
	require.Equal(t, uint64(0), sqlParam.Tail.IgnoredLines)

	esqlParam := ForeignExternParam("esql")
	require.Equal(t, uint64(1), esqlParam.Tail.IgnoredLines)
	require.NotNil(t, esqlParam.Tail.Fields.EscapedBy)
	require.Equal(t, byte(0), esqlParam.Tail.Fields.EscapedBy.Value)
}

// TestForeignScanISO8601Timestamp proves an ESQL scan parses ES's native
// ISO 8601 UTC dates ("...T...Z") into a declared timestamp column, and that
// External.Prepare rehydrates the synthetic Extern param from ForeignScan
// (the remote-decode path arrives with Extern == nil).
func TestForeignScanISO8601Timestamp(t *testing.T) {
	proc := testutil.NewProcess(t)
	ses := &fakeScanSession{}
	proc.Session = ses
	cfg := `{"addresses":["http://unused"]}`
	conn := &fakeScanConn{kind: foreigntvf.KindESQL,
		csv: "name,hired\r\nDave,2023-06-15T08:30:00.123Z\r\n"}
	ses.PutForeignConn(context.TODO(), foreigntvf.MakeHandle(foreigntvf.KindESQL, cfg), conn)

	ts3 := types.New(types.T_timestamp, 0, 3)
	cols := []*plan.ColDef{
		{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: "hired", Typ: plan.Type{Id: int32(types.T_timestamp), Scale: 3}},
	}
	param := foreignScanParam(t, "esql", cfg, cols, []string{"name", "hired"})
	param.Fileparam.Filepath = "FROM employees"

	r := NewForeignScanReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"name", "hired"}
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(ts3)
	defer bat.Clean(proc.Mp())
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 1, bat.RowCount())
	require.False(t, bat.Vecs[1].GetNulls().Contains(0))
	require.NoError(t, r.Close())

	// Prepare-time rehydration: Extern nil + ForeignScan set -> synthetic
	// param rebuilt with the ESQL dialect.
	op := NewArgument()
	op.Es = &ExternalParam{}
	op.Es.ForeignScan = &plan.ForeignScan{Kind: "esql"}
	op.Es.FileList = []string{"q"}
	op.Es.Cols = cols
	attrs := make([]plan.ExternAttr, len(cols))
	for i, col := range cols {
		attrs[i] = plan.ExternAttr{ColName: col.Name, ColIndex: int32(i), ColFieldIndex: int32(i)}
	}
	op.Es.Attrs = attrs
	op.Es.Fileparam = &ExFileparam{}
	require.NoError(t, op.Prepare(proc))
	require.NotNil(t, op.Es.Extern)
	require.Equal(t, uint64(1), op.Es.Extern.Tail.IgnoredLines)
	op.Free(proc, false, nil)
}
