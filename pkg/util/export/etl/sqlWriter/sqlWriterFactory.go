// Copyright 2022 Matrix Origin
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

package sqlWriter

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	errNotReady = moerr.NewInvalidStateNoCtx("SQL writer's DB conn not ready")
)

// sqlWriterDBUser holds the db user for logger
var (
	sqlWriterDBUser atomic.Value
	dbAddressFunc   atomic.Value

	sqlWriter = &BaseSqlWriter{}

	once sync.Once
)

const MOLoggerUser = "mo_logger"

type DBUser struct {
	UserName string
	Password string
}

func SetSQLWriterDBUser(userName string, password string) {
	user := &DBUser{
		UserName: userName,
		Password: password,
	}
	sqlWriterDBUser.Store(user)
}
func GetSQLWriterDBUser() (*DBUser, error) {
	dbUser := sqlWriterDBUser.Load()
	if dbUser == nil {
		return nil, errNotReady
	} else {
		return sqlWriterDBUser.Load().(*DBUser), nil

	}
}

func SetSQLWriterDBAddressFunc(f func(context.Context) (string, error)) {
	dbAddressFunc.Store(f)
}

func GetSQLWriterDBAddressFunc() func(context.Context) (string, error) {
	return dbAddressFunc.Load().(func(context.Context) (string, error))
}

func NewSqlWriter(ctx context.Context) *BaseSqlWriter {
	once.Do(func() {
		sqlWriter = &BaseSqlWriter{
			ctx:          ctx,
			forceNewConn: true,
		}
	})
	return sqlWriter
}
