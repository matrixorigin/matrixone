// Copyright 2021 - 2024 Matrix Origin
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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func Test_mysqlResp(t *testing.T) {
	var resp *MysqlResp
	resp.SetStr(DBNAME, "test")
	resp.GetStr(DBNAME)
	resp.SetU32(CONNID, 100)
	resp.GetU32(CONNID)
	resp.ResetStatistics()
}

func TestIsRowCountDML(t *testing.T) {
	require.True(t, isRowCountDML(&tree.Insert{}))
	require.True(t, isRowCountDML(&tree.Update{}))
	require.True(t, isRowCountDML(&tree.Delete{}))
	require.True(t, isRowCountDML(&tree.Replace{}))
	require.True(t, isRowCountDML(&tree.Load{}))

	require.False(t, isRowCountDML(&tree.Select{}))
	require.False(t, isRowCountDML(&tree.CreateTable{}))
	require.False(t, isRowCountDML(nil))
}

func TestSessionLastAffectedRows(t *testing.T) {
	ses := &Session{}
	require.Equal(t, int64(0), ses.GetLastAffectedRows())

	ses.SetLastAffectedRows(7)
	require.Equal(t, int64(7), ses.GetLastAffectedRows())

	// -1 sentinel used after result-set statements.
	ses.SetLastAffectedRows(-1)
	require.Equal(t, int64(-1), ses.GetLastAffectedRows())
}
