// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildMoDatabaseBatchQuery(t *testing.T) {
	require.Equal(t, MoDatabaseBatchQuery, BuildMoDatabaseBatchQuery(nil))
	require.Equal(
		t,
		MoDatabaseBatchQuery+" and "+SystemDBAttr_AccID+" in (0,10)",
		BuildMoDatabaseBatchQuery([]uint32{0, 10}),
	)
}

func TestBuildMoTablesBatchQuery(t *testing.T) {
	require.Equal(t, MoTablesBatchQuery, BuildMoTablesBatchQuery(nil))
	require.Equal(
		t,
		MoTablesBatchQuery+" and "+SystemRelAttr_AccID+" in (1)",
		BuildMoTablesBatchQuery([]uint32{1}),
	)
}

func TestBuildMoColumnsBatchQuery(t *testing.T) {
	require.Equal(t, MoColumnsBatchQuery, BuildMoColumnsBatchQuery(nil))
	require.Equal(
		t,
		strings.Replace(MoColumnsBatchQuery, " order by ", " and "+SystemColAttr_AccID+" in (1,2) order by ", 1),
		BuildMoColumnsBatchQuery([]uint32{1, 2}),
	)
}
