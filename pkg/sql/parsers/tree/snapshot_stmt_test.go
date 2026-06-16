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

// ---- DropSnapShot.Format with publication info ----

func TestDropSnapShot_Format_WithPublicationInfo(t *testing.T) {
	node := NewDropSnapShot(false, "snap1", "acc1", "pub1")
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	node.Format(ctx)
	require.Equal(t, "drop snapshot snap1 from acc1 publication pub1", ctx.String())
	node.Free()
}

func TestDropSnapShot_Format_WithoutPublicationInfo(t *testing.T) {
	node := NewDropSnapShot(true, "snap2", "", "")
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	node.Format(ctx)
	require.Equal(t, "drop snapshot if exists snap2", ctx.String())
	node.Free()
}

// ---- ObjectList.Format with SubscriptionAccountName and PubName ----

func TestObjectList_Format_WithSubscription(t *testing.T) {
	stmt := NewObjectList()
	stmt.Snapshot = Identifier("snap1")
	stmt.SubscriptionAccountName = "sys"
	stmt.PubName = Identifier("mypub")
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Contains(t, ctx.String(), "FROM sys PUBLICATION mypub")
	stmt.Free()
}

// ---- GetDdl.Format with SubscriptionAccountName and PubName ----

func TestGetDdl_Format_WithSubscription(t *testing.T) {
	stmt := NewGetDdl()
	dbName := Identifier("testdb")
	stmt.Database = &dbName
	stmt.SubscriptionAccountName = "acc1"
	pubName := Identifier("pub1")
	stmt.PubName = &pubName
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Contains(t, ctx.String(), "FROM acc1 PUBLICATION pub1")
	stmt.Free()
}

// ---- CreateSnapShot.Format with ObjectInfo publication info ----

func TestCreateSnapShot_Format_WithPublicationInfo(t *testing.T) {
	node := &CreateSnapShot{
		Name: "snap1",
		Object: ObjectInfo{
			SLevel:      SnapshotLevelType{Level: SNAPSHOTLEVELDATABASE},
			ObjName:     Identifier("mydb"),
			AccountName: Identifier("acc1"),
			PubName:     Identifier("pub1"),
		},
	}
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	node.Format(ctx)
	result := ctx.String()
	require.Contains(t, result, "create snapshot snap1 for")
	require.Contains(t, result, "from acc1 publication pub1")
}
