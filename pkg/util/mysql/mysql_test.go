// Copyright 2021 - 2022 Matrix Origin
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

package mysql

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemMySQL(t *testing.T) {
	m, err := NewMySQLServer(12345, "root", "root")
	require.NoError(t, err)

	require.NoError(t, m.Start())
	defer func() {
		assert.NoError(t, m.Stop())
	}()

	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:12345)/")
	require.NoError(t, err)

	_, err = db.Exec("create database if not exists mo_task")
	require.NoError(t, err)

	_, err = db.Exec("create table if not exists mo_task.t1 (c1 int primary key, c2 int unique)")
	require.NoError(t, err)

	_, err = db.Exec("create table if not exists mo_task.t1 (c1 int primary key, c2 int unique)")
	require.NoError(t, err)

	res, err := db.Exec("insert into mo_task.t1(c1, c2) values(1, 1)")
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	res, err = db.Exec("insert into mo_task.t1(c1, c2) values(2, 1)")
	require.Error(t, err)
	assert.Nil(t, res)
}
