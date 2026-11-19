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
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func floatResolverFixture(t testing.TB, name string) (*planpb.TableDef, *planpb.TableDef) {
	t.Helper()
	data, err := os.ReadFile("../sql/plan/testdata/legacy_float_resolver.json")
	require.NoError(t, err)
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	source, expected := new(planpb.TableDef), new(planpb.TableDef)
	require.Contains(t, fixtures, name+"_source")
	require.NoError(t, source.Unmarshal(fixtures[name+"_source"]))
	require.NoError(t, expected.Unmarshal(fixtures[name+"_execution"]))
	return source, expected
}

func floatResolver(t testing.TB, source *planpb.TableDef) func() (*planpb.TableDef, error) {
	t.Helper()
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.GetSessionInfo().LCTimeNames = "en_US"
	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().GetTableDef(gomock.Any()).Return(source).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(42)).Return("db", "t", relation, nil).AnyTimes()
	ses, _ := newObservedProtocolSession()
	ses.txnHandler = InitTxnHandler("", eng, proc.Ctx, txnOp)
	c := &TxnCompilerContext{execCtx: &ExecCtx{reqCtx: proc.Ctx, ses: ses, proc: proc}}
	return func() (*planpb.TableDef, error) { _, def, err := c.ResolveById(42, nil); return def, err }
}

func TestLegacyFloatResolverRepeatedCatalog(t *testing.T) {
	for _, name := range []string{"ordinary", "temporal_flat", "temporal_locale30", "nested_precision8"} {
		t.Run(name, func(t *testing.T) {
			source, expected := floatResolverFixture(t, name)
			before := proto.Clone(source)
			resolve := floatResolver(t, source)
			for range 2 {
				actual, err := resolve()
				require.NoError(t, err)
				require.True(t, proto.Equal(expected, actual), "resolver must retain repaired tree and frozen DEFAULT values")
				require.NotSame(t, source, actual)
				if name != "ordinary" {
					require.NotSame(t, source.Cols[1].Default.Expr, actual.Cols[1].Default.Expr)
					// A later resolution must not observe mutations of a caller's tree.
					actual.Cols[1].Default.Expr.Typ.Width++
				}
			}
			require.True(t, proto.Equal(before, source), "resolution must not mutate the source catalog")
		})
	}
}

// Use -benchtime=1x with /unprimed$ to measure the first resolution on a fresh
// compiler context. /repeated$ primes the same source once before timing, and
// every measured operation resolves the original catalog again (not the result).
func BenchmarkLegacyFloatResolver(b *testing.B) {
	for _, name := range []string{"ordinary", "temporal_flat", "temporal_locale30", "nested_precision8"} {
		for _, primed := range []bool{false, true} {
			mode := "unprimed"
			if primed {
				mode = "repeated"
			}
			b.Run(name+"/"+mode, func(b *testing.B) {
				source, expected := floatResolverFixture(b, name)
				resolve := floatResolver(b, source)
				if primed {
					actual, err := resolve()
					require.NoError(b, err)
					require.True(b, proto.Equal(expected, actual))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					actual, err := resolve()
					if err != nil {
						b.Fatal(err)
					}
					if actual == nil || len(actual.Cols) != len(expected.Cols) {
						b.Fatal("lost catalog columns")
					}
				}
			})
		}
	}
}
