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
	"fmt"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"go.uber.org/multierr"
)

// MySQLServer mysql sql server in memory, just used for testing.
type MySQLServer interface {
	// Start start mysql server
	Start() error
	// Stop stop mysql server
	Stop() error
}

type mysqlServer struct {
	s      *server.Server
	engine *sqle.Engine
}

func NewMySQLServer(port int, name, password string) (MySQLServer, error) {
	sql.NewDatabaseProvider()
	engine := sqle.NewDefault(
		memory.NewMemoryDBProvider(
			information_schema.NewInformationSchemaDatabase(),
		))
	engine.Analyzer.Catalog.MySQLDb.AddSuperUser(name, "localhost", password)
	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("127.0.0.1:%d", port),
	}
	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		return nil, multierr.Append(err, engine.Close())
	}
	return &mysqlServer{s: s, engine: engine}, nil
}

func (s *mysqlServer) Start() error {
	go s.s.Start()
	return nil
}

func (s *mysqlServer) Stop() error {
	_ = s.engine.Close()
	return s.s.Close()
}
