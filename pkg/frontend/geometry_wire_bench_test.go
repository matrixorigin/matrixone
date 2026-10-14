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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"math"
	"reflect"
	"testing"
)

var wireBenchSink []byte

func wireBenchGeometries() []struct {
	name string
	g    geo.Geometry
} {
	result := make([]struct {
		name string
		g    geo.Geometry
	}, 1, 3)
	result[0].name, result[0].g = "point", geo.Point{X: 1, Y: 2}
	for _, n := range []int{1024, 4096} {
		ring := make([]geo.Coord, n+1)
		for i := 0; i < n; i++ {
			a := 2 * math.Pi * float64(i) / float64(n)
			ring[i] = geo.Coord{X: math.Cos(a), Y: math.Sin(a)}
		}
		ring[n] = ring[0]
		result = append(result, struct {
			name string
			g    geo.Geometry
		}{fmt.Sprint(n), geo.Polygon{Rings: [][]geo.Coord{ring}}})
	}
	return result
}
func BenchmarkGeometryWireEncoding(b *testing.B) {
	for _, fixture := range wireBenchGeometries() {
		wkb := geo.WriteWKB(fixture.g)
		compact, err := geo.WriteWKBFloat32(fixture.g)
		if err != nil {
			b.Fatal(err)
		}
		text := []byte(geo.WriteWKT(fixture.g))
		for _, input := range []struct {
			name    string
			payload []byte
			oid     types.T
		}{{"wkb64", wkb, types.T_geometry}, {"wkb32", compact, types.T_geometry32}, {"cached-wkt", text, types.T_geometry}} {
			b.Run(fixture.name+"/"+input.name, func(b *testing.B) {
				typ := input.oid.ToType()
				typ.Width = 4327
				got, err := encodeMySQLGeometry(input.payload, typ)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) < 9 {
					b.Fatal("empty encoding")
				}
				b.SetBytes(int64(len(got)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					wireBenchSink, err = encodeMySQLGeometry(input.payload, typ)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

type wireBenchDiscardConn struct{ testConn }

func (c *wireBenchDiscardConn) Write(p []byte) (int, error) { return len(p), nil }
func wireBenchProtocol(b *testing.B) *MysqlProtocolImpl {
	b.Helper()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		b.Fatal(err)
	}
	setSessionAlloc("", NewLeakCheckAllocator())
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setPu("", pu)
	conn, err := NewIOSession(&wireBenchDiscardConn{}, pu, "")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := conn.Close(); err != nil {
			b.Error(err)
		}
	})
	proto := NewMysqlClientProtocol("", 0, conn, 1024, sv)
	proto.SetSession(&Session{feSessionImpl: feSessionImpl{txnHandler: &TxnHandler{}}})
	b.Cleanup(proto.Close)
	return proto
}
func BenchmarkGeometryMaterializedRow(b *testing.B) {
	for _, fixture := range wireBenchGeometries() {
		for _, binaryRow := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/binary=%v", fixture.name, binaryRow), func(b *testing.B) {
				proto := wireBenchProtocol(b)
				ctx := context.Background()
				mrs := &MysqlResultSet{}
				for _, typ := range []types.Type{types.T_int64.ToType(), types.T_geometry.ToType(), types.T_varchar.ToType()} {
					col := new(MysqlColumn)
					if err := setMysqlColumnTypeInfo(ctx, typ, col); err != nil {
						b.Fatal(err)
					}
					mrs.AddColumn(col)
				}
				mrs.AddRow([]any{int64(7), []byte(geo.WriteWKT(fixture.g)), nil})
				write := func() error {
					var err error
					if binaryRow {
						err = proto.appendResultSetBinaryRow(mrs, 0)
					} else {
						err = proto.appendResultSetTextRow(mrs, 0)
					}
					if err != nil {
						return err
					}
					return proto.flush()
				}
				if err := write(); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := write(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
func BenchmarkOrdinaryResultOutput(b *testing.B) {
	for _, n := range []int{4, 128} {
		for _, binaryRow := range []bool{false, true} {
			b.Run(fmt.Sprintf("cols=%d/binary=%v", n, binaryRow), func(b *testing.B) {
				proto := wireBenchProtocol(b)
				mrs := &MysqlResultSet{}
				row := make([]any, n)
				for i := range n {
					col := new(MysqlColumn)
					if err := setMysqlColumnTypeInfo(context.Background(), types.T_int64.ToType(), col); err != nil {
						b.Fatal(err)
					}
					mrs.AddColumn(col)
					row[i] = int64(i)
				}
				mrs.AddRow(row)
				write := func() error {
					var err error
					if binaryRow {
						err = proto.appendResultSetBinaryRow(mrs, 0)
					} else {
						err = proto.appendResultSetTextRow(mrs, 0)
					}
					if err != nil {
						return err
					}
					return proto.flush()
				}
				if err := write(); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := write(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

var wireBenchColumnSink *MysqlColumn

func BenchmarkOrdinaryColumnMetadata(b *testing.B) {
	b.Logf("MysqlColumn size=%d", reflect.TypeOf(MysqlColumn{}).Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col := new(MysqlColumn)
		if err := setMysqlColumnTypeInfo(context.Background(), types.T_int64.ToType(), col); err != nil {
			b.Fatal(err)
		}
		wireBenchColumnSink = col
	}
}
