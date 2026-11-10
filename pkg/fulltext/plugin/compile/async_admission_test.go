// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type asyncAdmissionContext struct {
	compileplugin.CompileContext
	table    *plan.TableDef
	run      func(string) error
	register func() error
}

func (c *asyncAdmissionContext) IndexInfo() *plan.CreateTable     { return nil }
func (c *asyncAdmissionContext) IsCCPRTaskTransaction() bool      { return false }
func (c *asyncAdmissionContext) OriginalTableDef() *plan.TableDef { return c.table }
func (c *asyncAdmissionContext) QryDatabase() string              { return "db`name" }
func (c *asyncAdmissionContext) RunSql(sql string) error          { return c.run(sql) }
func (c *asyncAdmissionContext) SinkerTypeFromAlgo(_ string) int8 { return 0 }
func (c *asyncAdmissionContext) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, _ bool, _ string, _ *plan.TableDef) error {
	return c.register()
}

func TestAsyncDatalinkAdmissionBeforeCDC(t *testing.T) {
	for _, tc := range []struct {
		name       string
		columnType types.T
		runErr     error
		cdcErr     error
	}{
		{name: "Datalink", columnType: types.T_datalink},
		{name: "ordinary text", columnType: types.T_text},
		{name: "invalid source", columnType: types.T_datalink, runErr: context.DeadlineExceeded},
		{name: "canceled", columnType: types.T_datalink, runErr: context.Canceled},
		{name: "CDC failure", columnType: types.T_datalink, cdcErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var events []string
			ctx := &asyncAdmissionContext{
				table: &plan.TableDef{Name: "table`name", Cols: []*plan.ColDef{
					{Name: "path`name", Typ: plan.Type{Id: int32(tc.columnType)}},
					{Name: "unindexed", Typ: plan.Type{Id: int32(types.T_datalink)}},
				}},
				run: func(sql string) error {
					events = append(events, "validate")
					require.Contains(t, sql, "sum(cast(_check_async_datalink_source(`path``name`,true) as unsigned))")
					require.Contains(t, sql, "from `db``name`.`table``name`")
					require.NotContains(t, sql, "unindexed")
					return tc.runErr
				},
				register: func() error { events = append(events, "CDC"); return tc.cdcErr },
			}
			err := (Hooks{}).HandleCreateIndex(ctx, map[string]*plan.IndexDef{"": {
				IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), IndexAlgoParams: `{"async":"true"}`, Parts: []string{"path`name"},
			}})
			if tc.runErr != nil {
				require.ErrorIs(t, err, tc.runErr)
				require.Equal(t, []string{"validate"}, events)
			} else {
				require.ErrorIs(t, err, tc.cdcErr)
				if tc.columnType == types.T_datalink {
					require.Equal(t, []string{"validate", "CDC"}, events)
				} else {
					require.Equal(t, []string{"CDC"}, events)
				}
			}
		})
	}
}
