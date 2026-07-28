// Copyright 2021 Matrix Origin
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

func TestNewCreateSubscription(t *testing.T) {
	cs := NewCreateSubscription(true, Identifier("testdb"), "", "127.0.0.1:6001", "sub_account", Identifier("pub1"), 60)
	require.NotNil(t, cs)
	require.True(t, cs.IsDatabase)
	require.Equal(t, Identifier("testdb"), cs.DbName)
	require.Equal(t, "", cs.TableName)
	require.Equal(t, "127.0.0.1:6001", cs.FromUri)
	require.Equal(t, "sub_account", cs.SubscriptionAccountName)
	require.Equal(t, Identifier("pub1"), cs.PubName)
	require.Equal(t, int64(60), cs.SyncInterval)
	require.Equal(t, "", cs.AccountName)
	require.False(t, cs.IfNotExists)
	cs.Free()
}

func TestCreateSubscription_SetAccountName(t *testing.T) {
	cs := NewCreateSubscription(true, Identifier(""), "", "127.0.0.1:6001", "", Identifier("pub1"), 0)
	require.Equal(t, "", cs.AccountName)
	cs.SetAccountName("acc1")
	require.Equal(t, "acc1", cs.AccountName)
	cs.Free()
}

func TestCreateSubscription_SetIfNotExists(t *testing.T) {
	cs := NewCreateSubscription(true, Identifier("testdb"), "", "127.0.0.1:6001", "", Identifier("pub1"), 0)
	require.False(t, cs.IfNotExists)
	cs.SetIfNotExists(true)
	require.True(t, cs.IfNotExists)
	cs.Free()
}

func TestCreateSubscription_Format(t *testing.T) {
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))

	tests := []struct {
		name                    string
		isDatabase              bool
		dbName                  Identifier
		tableName               string
		fromUri                 string
		subscriptionAccountName string
		pubName                 Identifier
		syncInterval            int64
		expected                string
	}{
		{
			name:                    "account-level subscription",
			isDatabase:              true,
			dbName:                  Identifier(""),
			tableName:               "",
			fromUri:                 "127.0.0.1:6001",
			subscriptionAccountName: "sub_acc",
			pubName:                 Identifier("pub1"),
			syncInterval:            0,
			expected:                "create account from '127.0.0.1:6001' sub_acc publication pub1",
		},
		{
			name:                    "database-level subscription",
			isDatabase:              true,
			dbName:                  Identifier("testdb"),
			tableName:               "",
			fromUri:                 "127.0.0.1:6001",
			subscriptionAccountName: "sub_acc",
			pubName:                 Identifier("pub1"),
			syncInterval:            0,
			expected:                "create database testdb from '127.0.0.1:6001' sub_acc publication pub1",
		},
		{
			name:                    "table-level subscription",
			isDatabase:              false,
			dbName:                  Identifier(""),
			tableName:               "testtable",
			fromUri:                 "127.0.0.1:6001",
			subscriptionAccountName: "sub_acc",
			pubName:                 Identifier("pub1"),
			syncInterval:            0,
			expected:                "create table testtable from '127.0.0.1:6001' sub_acc publication pub1",
		},
		{
			name:                    "database subscription with sync_interval",
			isDatabase:              true,
			dbName:                  Identifier("testdb"),
			tableName:               "",
			fromUri:                 "192.168.1.100:6001",
			subscriptionAccountName: "my_sub_acc",
			pubName:                 Identifier("mypub"),
			syncInterval:            120,
			expected:                "create database testdb from '192.168.1.100:6001' my_sub_acc publication mypub sync_interval = 120",
		},
		{
			name:                    "table subscription with sync_interval",
			isDatabase:              false,
			dbName:                  Identifier(""),
			tableName:               "orders",
			fromUri:                 "10.0.0.1:6001",
			subscriptionAccountName: "order_sub",
			pubName:                 Identifier("orderspub"),
			syncInterval:            30,
			expected:                "create table orders from '10.0.0.1:6001' order_sub publication orderspub sync_interval = 30",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewCreateSubscription(tt.isDatabase, tt.dbName, tt.tableName, tt.fromUri, tt.subscriptionAccountName, tt.pubName, tt.syncInterval)
			ctx.Reset()
			cs.Format(ctx)
			require.Equal(t, tt.expected, ctx.String())
			cs.Free()
		})
	}
}

func TestCreateSubscription_GetStatementType(t *testing.T) {
	cs := NewCreateSubscription(true, Identifier("testdb"), "", "127.0.0.1:6001", "", Identifier("pub1"), 0)
	require.Equal(t, "Create Subscription", cs.GetStatementType())
	cs.Free()
}

func TestCreateSubscription_GetQueryType(t *testing.T) {
	cs := NewCreateSubscription(true, Identifier("testdb"), "", "127.0.0.1:6001", "", Identifier("pub1"), 0)
	require.Equal(t, QueryTypeDCL, cs.GetQueryType())
	cs.Free()
}

func TestCreateSubscription_StmtKind(t *testing.T) {
	cs := NewCreateSubscription(true, Identifier("testdb"), "", "127.0.0.1:6001", "", Identifier("pub1"), 0)
	require.Equal(t, frontendStatusTyp, cs.StmtKind())
	cs.Free()
}

func TestCreateSubscription_TypeName(t *testing.T) {
	cs := CreateSubscription{}
	require.Equal(t, "tree.CreateSubscription", cs.TypeName())
}
