// Copyright 2025 Matrix Origin
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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// ---- ShowCcprSubscriptions.Format ----

func TestShowCcprSubscriptions_Format_WithTaskId(t *testing.T) {
	node := &ShowCcprSubscriptions{TaskId: "task-123"}
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	node.Format(ctx)
	require.Equal(t, "show ccpr subscription task-123", ctx.String())
}

func TestShowCcprSubscriptions_Format_WithoutTaskId(t *testing.T) {
	node := &ShowCcprSubscriptions{}
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	node.Format(ctx)
	require.Equal(t, "show ccpr subscriptions", ctx.String())
}

func TestShowCcprSubscriptions_StmtKind(t *testing.T) {
	node := &ShowCcprSubscriptions{}
	require.Equal(t, compositeResRowType, node.StmtKind())
}

func TestShowCcprSubscriptions_StatementType(t *testing.T) {
	node := &ShowCcprSubscriptions{}
	require.Equal(t, "Show Ccpr Subscriptions", node.GetStatementType())
	require.Equal(t, QueryTypeOth, node.GetQueryType())
}

// ---- GetObject.Format with publication info ----

func TestGetObject_Format_WithPublicationInfo(t *testing.T) {
	stmt := NewGetObject()
	stmt.ObjectName = Identifier("obj_001")
	stmt.ChunkIndex = 3
	stmt.SubscriptionAccountName = "sys"
	stmt.PubName = Identifier("mypub")

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "GET OBJECT obj_001 OFFSET 3 FROM sys PUBLICATION mypub", ctx.String())
	stmt.Free()
}

func TestGetObject_Format_WithoutPublicationInfo(t *testing.T) {
	stmt := NewGetObject()
	stmt.ObjectName = Identifier("obj_002")
	stmt.ChunkIndex = 0

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "GET OBJECT obj_002 OFFSET 0", ctx.String())
	stmt.Free()
}

// ---- GetDdl.Format with publication info ----

func TestGetDdl_Format_WithPublicationInfo(t *testing.T) {
	stmt := NewGetDdl()
	dbName := Identifier("testdb")
	stmt.Database = &dbName
	stmt.SubscriptionAccountName = "acc1"
	pubName := Identifier("pub1")
	stmt.PubName = &pubName

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "GETDDL DATABASE testdb FROM acc1 PUBLICATION pub1", ctx.String())
	stmt.Free()
}

func TestGetDdl_Format_WithoutPublicationInfo(t *testing.T) {
	stmt := NewGetDdl()
	dbName := Identifier("db1")
	stmt.Database = &dbName

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "GETDDL DATABASE db1", ctx.String())
	stmt.Free()
}

// ---- ObjectInfo.Format with publication info ----

func TestObjectInfo_Format_WithPublicationInfo(t *testing.T) {
	node := &ObjectInfo{
		SLevel:      SnapshotLevelType{Level: SNAPSHOTLEVELDATABASE},
		ObjName:     Identifier("mydb"),
		AccountName: Identifier("acc1"),
		PubName:     Identifier("pub1"),
	}
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	node.Format(ctx)
	result := ctx.String()
	require.Contains(t, result, "mydb")
	require.Contains(t, result, "from")
	require.Contains(t, result, "acc1")
	require.Contains(t, result, "publication")
	require.Contains(t, result, "pub1")
}

func TestObjectInfo_Format_WithoutPublicationInfo(t *testing.T) {
	node := &ObjectInfo{
		SLevel:  SnapshotLevelType{Level: SNAPSHOTLEVELDATABASE},
		ObjName: Identifier("mydb"),
	}
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	node.Format(ctx)
	result := ctx.String()
	require.Contains(t, result, "mydb")
	require.NotContains(t, result, "from")
}
