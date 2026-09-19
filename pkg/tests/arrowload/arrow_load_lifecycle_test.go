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

package arrowload

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestArrowLoadPermissionDeniedDoesNotReadOrWrite proves that target-table
// authorization is completed before an Arrow S3 source is inspected. The
// proxy observes the real MinIO protocol path, so a denied request cannot pass
// merely because the target table remains unchanged.
func TestArrowLoadPermissionDeniedDoesNotReadOrWrite(t *testing.T) {
	c := startArrowLoadCluster(t, 1, true, true, false)
	ownerDB := openArrowLoadDB(t, c, 0)

	const (
		databaseName = "arrow_load_permission"
		tableName    = "permission_target"
		roleName     = "arrow_load_permission_role"
		userName     = "arrow_load_permission_user"
		objectKey    = "permission/source.arrow"
	)

	mustExec(t, ownerDB, "drop user if exists "+userName)
	mustExec(t, ownerDB, "drop role if exists "+roleName)
	mustExec(t, ownerDB, "drop database if exists "+databaseName)
	t.Cleanup(func() {
		_, _ = ownerDB.Exec("drop user if exists " + userName)
		_, _ = ownerDB.Exec("drop role if exists " + roleName)
		_, _ = ownerDB.Exec("drop database if exists " + databaseName)
	})

	mustExec(t, ownerDB, "create database "+databaseName)
	mustExec(t, ownerDB, "create table `"+databaseName+"`.`"+tableName+"` (id bigint not null, name varchar(50))")
	mustExec(t, ownerDB, "insert into `"+databaseName+"`.`"+tableName+"` values (0, 'seed')")

	mustExec(t, ownerDB, "create role "+roleName)
	mustExec(t, ownerDB, "grant connect on account * to "+roleName)
	mustExec(t, ownerDB, "create user "+userName+" identified by '111' default role "+roleName)
	mustExec(t, ownerDB, "grant "+roleName+" to "+userName)

	server := startArrowLoadMinIO(t)
	fixturePath := fixtureIDName(t, t.TempDir(), "source.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "loaded"}}})
	server.put(t, objectKey, mustReadFile(t, fixturePath))

	var objectRequest atomic.Bool
	proxyEndpoint := startArrowMinIOProxy(t, server.endpointURL,
		func(http.ResponseWriter, *http.Request) bool {
			objectRequest.Store(true)
			return false
		})

	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	userDB, err := sql.Open("mysql", fmt.Sprintf(
		"%s:111@tcp(127.0.0.1:%d)/%s", userName,
		cn.GetServiceConfig().CN.Frontend.Port, databaseName))
	require.NoError(t, err)
	t.Cleanup(func() { _ = userDB.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, userDB.PingContext(ctx))

	loadSQL := minioLoadSQL(proxyEndpoint, server, objectKey,
		"`"+databaseName+"`.`"+tableName+"`", containerFile, false)
	objectRequest.Store(false)
	_, err = userDB.ExecContext(ctx, loadSQL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "do not have privilege to execute the statement")
	require.False(t, objectRequest.Load(), "denied Arrow LOAD must not access the MinIO object")
	require.Equal(t, int64(1), queryCount(t, ownerDB,
		"select count(*) from `"+databaseName+"`.`"+tableName+"`"))

	prepareSQL := "prepare arrow_load_permission_stmt from '" +
		strings.ReplaceAll(loadSQL, "'", "''") + "'"
	objectRequest.Store(false)
	_, err = userDB.ExecContext(ctx, prepareSQL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "do not have privilege to execute the statement")
	require.False(t, objectRequest.Load(), "denied prepared Arrow LOAD must not access the MinIO object")

	mustExec(t, ownerDB, "grant insert on table `"+databaseName+"`.`"+tableName+"` to "+roleName)
	objectRequest.Store(false)
	_, err = userDB.ExecContext(ctx, loadSQL)
	require.NoError(t, err)
	require.True(t, objectRequest.Load(), "authorized Arrow LOAD must access the MinIO object")
	require.Equal(t, int64(2), queryCount(t, ownerDB,
		"select count(*) from `"+databaseName+"`.`"+tableName+"`"))
}
