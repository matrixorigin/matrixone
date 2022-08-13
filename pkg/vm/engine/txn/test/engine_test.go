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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEngine(t *testing.T) {

	env := newEnv()
	defer func() {
		if err := env.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	tx := env.NewTx()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err := tx.Exec(ctx, `
    create table foo (
      a int primary key,
      b int
    )
  `)
	assert.Nil(t, err)

	err = tx.Exec(ctx, `insert into foo (a, b) values (1, 2)`)
	assert.Nil(t, err)

	err = tx.Exec(ctx, `select a, b from foo where b = 2`)
	assert.Nil(t, err)

	err = tx.Exec(ctx, `update foo set b = 3 where a = 1`)
	assert.Nil(t, err)

	err = tx.Exec(ctx, `delete from foo where a = 1`)
	assert.Nil(t, err)

	err = tx.Exec(ctx, `drop table foo`)
	assert.Nil(t, err)

	err = tx.Commit(ctx)
	assert.Nil(t, err)

}
