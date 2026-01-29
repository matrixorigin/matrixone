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

package table_function

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	arg := TableFunction{FuncName: "unnest"}
	arg.String(bytes.NewBuffer(nil))
}

func TestPrepare(t *testing.T) {
	arg := TableFunction{FuncName: "unnest",
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}
	err := arg.Prepare(testutil.NewProc(t))
	require.Error(t, err)
	arg.FuncName = "generate_series"
	err = arg.Prepare(testutil.NewProc(t))
	require.NoError(t, err)
	arg.FuncName = "metadata_scan"
	err = arg.Prepare(testutil.NewProc(t))
	require.NoError(t, err)
	arg.FuncName = "not_exist"
	err = arg.Prepare(testutil.NewProc(t))
	require.Error(t, err)
}

func TestParseTablePathWithAccount(t *testing.T) {
	tests := []struct {
		name              string
		path              string
		currentDatabase   string
		currentAccountId  uint32
		expectedDb        string
		expectedTable     string
		expectedAccountId uint32
		expectError       bool
		errorContains     string
	}{
		{
			name:              "single part - table only",
			path:              "mytable",
			currentDatabase:   "mydb",
			currentAccountId:  1,
			expectedDb:        "mydb",
			expectedTable:     "mytable",
			expectedAccountId: 1,
			expectError:       false,
		},
		{
			name:             "single part - no database selected",
			path:             "mytable",
			currentDatabase:  "",
			currentAccountId: 1,
			expectError:      true,
			errorContains:    "no database selected",
		},
		{
			name:              "two parts - db.table",
			path:              "mydb.mytable",
			currentDatabase:   "otherdb",
			currentAccountId:  1,
			expectedDb:        "mydb",
			expectedTable:     "mytable",
			expectedAccountId: 1,
			expectError:       false,
		},
		{
			name:              "three parts - db.table.account_id (same account)",
			path:              "mydb.mytable.1",
			currentDatabase:   "otherdb",
			currentAccountId:  1,
			expectedDb:        "mydb",
			expectedTable:     "mytable",
			expectedAccountId: 1,
			expectError:       false,
		},
		{
			name:              "three parts - db.table.account_id (sys account queries other)",
			path:              "mydb.mytable.5",
			currentDatabase:   "otherdb",
			currentAccountId:  0, // sys account
			expectedDb:        "mydb",
			expectedTable:     "mytable",
			expectedAccountId: 5,
			expectError:       false,
		},
		{
			name:             "three parts - non-sys account queries other (should fail)",
			path:             "mydb.mytable.2",
			currentDatabase:  "otherdb",
			currentAccountId: 1, // non-sys account
			expectError:      true,
			errorContains:    "only sys account can query stats for other accounts",
		},
		{
			name:             "three parts - invalid account_id format",
			path:             "mydb.mytable.abc",
			currentDatabase:  "otherdb",
			currentAccountId: 1,
			expectError:      true,
			errorContains:    "invalid account_id",
		},
		{
			name:             "four parts - too many",
			path:             "mydb.mytable.1.extra",
			currentDatabase:  "otherdb",
			currentAccountId: 1,
			expectError:      true,
			errorContains:    "invalid table path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			proc.GetSessionInfo().Database = tt.currentDatabase
			proc.Ctx = defines.AttachAccountId(proc.Ctx, tt.currentAccountId)

			db, table, accountId, err := parseTablePathWithAccount(tt.path, proc)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedDb, db)
				require.Equal(t, tt.expectedTable, table)
				require.Equal(t, tt.expectedAccountId, accountId)
			}
		})
	}
}
