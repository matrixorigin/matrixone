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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// resetOptimizerHintsOnCN restores the shared service-runtime value, not only
// one session variable. Use a fresh connection and context so cleanup remains
// independent of the connection and deadline exercised by the test.
func resetOptimizerHintsOnCN(t *testing.T, port int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	for _, statement := range []string{
		"set role moadmin",
		`set session optimizer_hints = ""`,
	} {
		_, err = db.ExecContext(ctx, statement)
		require.NoErrorf(t, err, "exec failed: %s", statement)
	}
}
