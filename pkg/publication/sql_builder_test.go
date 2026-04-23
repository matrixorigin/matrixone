// Copyright 2024 Matrix Origin
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

package publication

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCcprSnapshotSQL_Default(t *testing.T) {
	sql := PublicationSQLBuilder.CreateCcprSnapshotSQL("snap1", "acc1", "pub1", "unknown", "db1", "t1")
	assert.Contains(t, sql, "snap1")
	assert.Contains(t, sql, "acc1")
	assert.Contains(t, sql, "pub1")
}

func TestCreateCcprSnapshotSQL_Account(t *testing.T) {
	sql := PublicationSQLBuilder.CreateCcprSnapshotSQL("snap1", "acc1", "pub1", "account", "", "")
	assert.Contains(t, sql, "snap1")
	assert.Contains(t, sql, "ACCOUNT")
}

func TestCreateCcprSnapshotSQL_Database(t *testing.T) {
	sql := PublicationSQLBuilder.CreateCcprSnapshotSQL("snap1", "acc1", "pub1", "database", "mydb", "")
	assert.Contains(t, sql, "mydb")
}

func TestCreateCcprSnapshotSQL_Table(t *testing.T) {
	sql := PublicationSQLBuilder.CreateCcprSnapshotSQL("snap1", "acc1", "pub1", "table", "mydb", "mytbl")
	assert.Contains(t, sql, "mydb")
	assert.Contains(t, sql, "mytbl")
}

func TestDropCcprSnapshotSQL(t *testing.T) {
	sql := PublicationSQLBuilder.DropCcprSnapshotSQL("snap1", "acc1", "pub1")
	assert.Contains(t, sql, "DROP SNAPSHOT")
	assert.Contains(t, sql, "snap1")
}

func TestQueryMoIndexesSQL(t *testing.T) {
	sql := PublicationSQLBuilder.QueryMoIndexesSQL(42, "acc1", "pub1", "snap1")
	assert.Contains(t, sql, "42")
	assert.Contains(t, sql, "acc1")
}

func TestObjectListSQL(t *testing.T) {
	sql := PublicationSQLBuilder.ObjectListSQL("snap1", "", "acc1", "pub1")
	assert.Contains(t, sql, "snap1")
	assert.Contains(t, sql, "-") // empty against becomes "-"
}

func TestObjectListSQL_WithAgainst(t *testing.T) {
	sql := PublicationSQLBuilder.ObjectListSQL("snap1", "snap0", "acc1", "pub1")
	assert.Contains(t, sql, "snap0")
	assert.NotContains(t, sql, "- ") // should not have placeholder
}

func TestGetObjectSQL(t *testing.T) {
	sql := PublicationSQLBuilder.GetObjectSQL("acc1", "pub1", "obj1", 3)
	assert.Contains(t, sql, "obj1")
	assert.Contains(t, sql, "3")
}

func TestGetDdlSQL(t *testing.T) {
	sql := PublicationSQLBuilder.GetDdlSQL("snap1", "acc1", "pub1", "account", "", "")
	assert.Contains(t, sql, "snap1")
	assert.Contains(t, sql, "account")
}

func TestGetDdlSQL_EmptySnapshotName(t *testing.T) {
	sql := PublicationSQLBuilder.GetDdlSQL("", "acc1", "pub1", "table", "db1", "t1")
	assert.Contains(t, sql, "-") // empty snapshot becomes placeholder
}

func TestQueryMoCcprLogSQL(t *testing.T) {
	sql := PublicationSQLBuilder.QueryMoCcprLogSQL("task-'uuid")
	assert.Contains(t, sql, "task-''uuid")
	assert.Contains(t, sql, "SELECT")
}

func TestQueryMoCcprLogFullSQL(t *testing.T) {
	sql := PublicationSQLBuilder.QueryMoCcprLogFullSQL("task-'uuid")
	assert.Contains(t, sql, "task-''uuid")
}

func TestQuerySnapshotTsSQL(t *testing.T) {
	sql := PublicationSQLBuilder.QuerySnapshotTsSQL("snap1", "acc1", "pub1")
	assert.Contains(t, sql, "snap1")
}

func TestGetDatabasesSQL(t *testing.T) {
	sql := PublicationSQLBuilder.GetDatabasesSQL("snap1", "acc1", "pub1", "account", "", "")
	assert.Contains(t, sql, "snap1")
}

func TestCheckSnapshotFlushedSQL(t *testing.T) {
	sql := PublicationSQLBuilder.CheckSnapshotFlushedSQL("snap1", "acc1", "pub1")
	assert.Contains(t, sql, "snap1")
}

func TestGCStatusSQL(t *testing.T) {
	sql := PublicationSQLBuilder.GCStatusSQL()
	assert.NotEmpty(t, sql)
}

func TestUpdateMoCcprLogSQL(t *testing.T) {
	sql := PublicationSQLBuilder.UpdateMoCcprLogSQL("task'1", 1, 100, "{}", "err", 2)
	assert.Contains(t, sql, "task''1")
}

func TestUpdateMoCcprLogNoContextSQL(t *testing.T) {
	sql := PublicationSQLBuilder.UpdateMoCcprLogNoContextSQL("task1", 3, 200, "some error", 3)
	assert.Contains(t, sql, "task1")
	assert.Contains(t, sql, "some error")
}

func TestQueryMoCcprLogStateBeforeUpdateSQL(t *testing.T) {
	sql := PublicationSQLBuilder.QueryMoCcprLogStateBeforeUpdateSQL("task'1")
	assert.Contains(t, sql, "task''1")
}

func TestUpdateMoCcprLogNoStateSQL(t *testing.T) {
	sql := PublicationSQLBuilder.UpdateMoCcprLogNoStateSQL("task1", 2, 1001, 12345, "{}", "")
	assert.Contains(t, sql, "task1")
	assert.Contains(t, sql, "12345")
}

func TestUpdateMoCcprLogNoStateNoContextSQL(t *testing.T) {
	sql := PublicationSQLBuilder.UpdateMoCcprLogNoStateNoContextSQL("task1", 1, 1000, 12345, "err msg")
	assert.Contains(t, sql, "task1")
	assert.Contains(t, sql, "err msg")
}

func TestUpdateMoCcprLogIterationStateAndCnUuidSQL(t *testing.T) {
	sql := PublicationSQLBuilder.UpdateMoCcprLogIterationStateAndCnUuidSQL("task'1", 1, "cn-uuid-1")
	assert.Contains(t, sql, "task''1")
	assert.Contains(t, sql, "cn-uuid-1")
}

func TestRegisterSyncProtectionSQL(t *testing.T) {
	sql := PublicationSQLBuilder.RegisterSyncProtectionSQL("job1", "base64bf", 100, 200, "task1")
	assert.Contains(t, sql, "job1")
	assert.Contains(t, sql, "base64bf")
}

func TestRenewSyncProtectionSQL(t *testing.T) {
	sql := PublicationSQLBuilder.RenewSyncProtectionSQL("job1", 300)
	assert.Contains(t, sql, "job1")
	assert.Contains(t, sql, "300")
}

func TestUnregisterSyncProtectionSQL(t *testing.T) {
	sql := PublicationSQLBuilder.UnregisterSyncProtectionSQL("job1")
	assert.Contains(t, sql, "job1")
}

func TestEscapeSQLString(t *testing.T) {
	assert.Equal(t, "hello", escapeSQLString("hello"))
	assert.Equal(t, "it''s", escapeSQLString("it's"))
	assert.Equal(t, "back\\\\slash", escapeSQLString("back\\slash"))
	assert.Equal(t, "it''s a \\\\path", escapeSQLString("it's a \\path"))
}

func TestEscapeOrPlaceholder(t *testing.T) {
	assert.Equal(t, "-", escapeOrPlaceholder(""))
	assert.Equal(t, "hello", escapeOrPlaceholder("hello"))
	assert.Equal(t, "it''s", escapeOrPlaceholder("it's"))
}

func TestPublicationSQLTemplates_OutputAttrs(t *testing.T) {
	// QueryMoIndexes has output attrs
	tmpl := PublicationSQLTemplates[PublicationQueryMoIndexesSqlTemplate_Idx]
	assert.True(t, len(tmpl.OutputAttrs) > 0)
	assert.Contains(t, strings.Join(tmpl.OutputAttrs, ","), "table_id")

	// QueryMoCcprLog has output attrs
	tmpl2 := PublicationSQLTemplates[PublicationQueryMoCcprLogSqlTemplate_Idx]
	assert.Contains(t, strings.Join(tmpl2.OutputAttrs, ","), "cn_uuid")

	// QueryMoCcprLogFull has output attrs
	tmpl3 := PublicationSQLTemplates[PublicationQueryMoCcprLogFullSqlTemplate_Idx]
	assert.Contains(t, strings.Join(tmpl3.OutputAttrs, ","), "subscription_name")
}
