// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestNativePADCDCSchemaBeforeTargetConnection(t *testing.T) {
	for _, sinkType := range []string{CDCSinkType_MO, CDCSinkType_MySQL} {
		t.Run(sinkType, func(t *testing.T) {
			connections := 0
			stub := gostub.Stub(&OpenDbConn, func(context.Context, string, string, string, int, string) (*sql.DB, error) {
				connections++
				return nil, errors.New("unexpected target connection")
			})
			defer stub.Reset()
			source := &plan.TableDef{Name: "n", Cols: []*plan.ColDef{{Name: "s",
				Typ: plan.Type{Id: int32(types.T_varchar), Width: 8, Charset: 3, CollationVersion: 1}}}}
			before := proto.Clone(source)
			sink, err := CreateMysqlSinker2(t.Context(), UriInfo{SinkTyp: sinkType}, 1, "task",
				&DbTableInfo{SinkDbName: "db", SinkTblName: "n"}, nil, source,
				0, time.Second, NewCdcActiveRoutine(), 1024, CDCDefaultSendSqlTimeout)
			require.Nil(t, sink)
			require.ErrorContains(t, err, "versioned collations")
			require.Zero(t, connections, "unsupported schema must not acquire target resources")
			require.Equal(t, before, source)
		})
	}
}

func TestNativePADCDCLegacyTargetDialect(t *testing.T) {
	for _, tc := range []struct{ sinkType, collation string }{
		{CDCSinkType_MO, "utf32_bin"},
		{CDCSinkType_MySQL, "utf8mb4_bin"},
	} {
		t.Run(tc.sinkType, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			stub := gostub.Stub(&OpenDbConn, func(context.Context, string, string, string, int, string) (*sql.DB, error) {
				return db, nil
			})
			defer stub.Reset()
			for range 3 {
				mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectClose()
			// Reuse the bounded recorder already used by sinker identifier tests.
			source := &plan.TableDef{Name: "bmsql_source", DefaultCharset: 2,
				Cols: []*plan.ColDef{
					{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{}},
					{Name: "s", OriginName: "s", Typ: plan.Type{Id: int32(types.T_varchar), Width: 8, Charset: 2},
						Default: &plan.Default{NullAbility: true}},
				},
				Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}}, Name2ColIndex: map[string]int32{"id": 0, "s": 1}}
			before := proto.Clone(source)
			sink, err := CreateMysqlSinker2(t.Context(), UriInfo{SinkTyp: tc.sinkType}, 1, "task",
				&DbTableInfo{SinkDbName: "db", SinkTblName: "n"}, nil, source,
				0, time.Second, NewCdcActiveRoutine(), 1024, CDCDefaultSendSqlTimeout)
			require.NoError(t, err)
			defer func() {
				sink.Close()
				require.NoError(t, mock.ExpectationsWereMet())
			}()
			statements := sink.(*mysqlSinker2).executor.debugTxnRecorder.txnSQL
			require.Len(t, statements, 3)
			require.Contains(t, statements[2], "COLLATE="+tc.collation)
			require.Equal(t, before, source)
		})
	}
}
