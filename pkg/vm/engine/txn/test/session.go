// Copyright 2022 Matrix Origin
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

package testtxnengine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type Session struct {
	env       *testEnv
	currentDB string
	currentTx *Tx
}

func (t *testEnv) NewSession() *Session {
	session := &Session{
		env:       t,
		currentDB: defaultDatabase,
	}
	return session
}

func (s *Session) Exec(ctx context.Context, filePath string, content string) (err error) {

	s.currentDB = defaultDatabase

	defer func() {
		if err == nil {
			return
		}

		//sqlError, ok := err.(*errors.SqlError)
		//if ok {
		//	fmt.Printf("ERROR: %s\n", sqlError.Error())
		//	err = nil
		//}

		//moError, ok := err.(*moerr.Error)
		//if ok {
		//	fmt.Printf("ERROR: %s\n", moError.Error())
		//	err = nil
		//}

		//posErr, ok := err.(mysql.PositionedErr)
		//if ok {
		//	fmt.Printf("ERROR: %s\n", content[posErr.Pos:])
		//	err = nil
		//}

		//TODO
		//fmt.Printf("ERROR: %s\n", content[posErr.Pos:])
		err = nil

	}()

	stmts, err := mysql.Parse(content)
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		stmtText := tree.String(stmt, dialect.MYSQL)
		//fmt.Printf("SQL: %s @%s\n", stmtText, filePath)

		if s.currentTx == nil {
			tx := s.NewTx()
			err := tx.Exec(ctx, stmtText, stmt)
			if err != nil {
				tx.Rollback(ctx)
				return err
			} else {
				if err := tx.Commit(ctx); err != nil {
					return err
				}
			}
			continue
		}

		if err := s.currentTx.Exec(ctx, stmtText, stmt); err != nil {
			s.currentTx.Rollback(ctx)
			s.currentTx = nil
			return err
		}

	}

	return nil
}
