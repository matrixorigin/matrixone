// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/stretchr/testify/require"
)

func TestCDCCreateTaskOptionsPreservePatternValidationError(t *testing.T) {
	const tables = "db1.t1:db2.t1,db1.t1:db2.t2"
	const expected = "internal error: one db/table: db1.t1 can't be used as multi sources in a cdc task"

	opts := &CDCCreateTaskOptions{}
	err := opts.handleLevel(context.Background(), nil, cdc.CDCPitrGranularity_Table, tables)
	require.EqualError(t, err, expected)
	require.NotContains(t, err.Error(), "invalid level")

	err = opts.handleFrequency(
		context.Background(), nil, cdc.CDCPitrGranularity_Table, "1h", tables,
	)
	require.EqualError(t, err, expected)
	require.NotContains(t, err.Error(), "invalid level")
}
