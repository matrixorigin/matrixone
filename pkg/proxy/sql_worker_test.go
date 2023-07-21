// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSqlWorker_InitDBNotReady(t *testing.T) {
	testWithServer(t, func(t *testing.T, addr string, server *Server) {
		w := newSQLWorker()
		db, err := w.initDB()
		require.Error(t, err)
		require.Nil(t, db)
	})
}

func TestSqlWorker_InitDB(t *testing.T) {
	testWithServer(t, func(t *testing.T, addr string, server *Server) {
		w := newSQLWorker()
		w.SetSQLUser("dump", "111")
		w.SetAddressFn(func(ctx context.Context, b bool) (string, error) {
			return addr, nil
		})
		db, err := w.initDB()
		require.NoError(t, err)
		require.NotNil(t, db)
		defer func() {
			_ = db.Close()
		}()
	})
}

func TestSqlWorker_GetCNServersByTenant(t *testing.T) {
	testWithServer(t, func(t *testing.T, addr string, server *Server) {
		w := newSQLWorker()
		w.SetSQLUser("dump", "111")
		w.SetAddressFn(func(ctx context.Context, b bool) (string, error) {
			return addr, nil
		})
		cns, err := w.GetCNServersByTenant("t1")
		require.NoError(t, err)
		require.Greater(t, len(cns), 0)
	})
}
