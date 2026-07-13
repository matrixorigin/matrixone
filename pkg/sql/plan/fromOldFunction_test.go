// Copyright 2021 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestExtractToDateReturnTypePrecision(t *testing.T) {
	tp, fsp := ExtractToDateReturnType("%Y-%m-%d %H:%i:%s.%f")
	require.Equal(t, types.T_datetime, tp)
	require.Equal(t, MaxFsp, fsp)

	tp, fsp = ExtractToDateReturnType("%Y-%m-%d %H:%i:%s")
	require.Equal(t, types.T_datetime, tp)
	require.Equal(t, 0, fsp)

	tp, fsp = ExtractToDateReturnType("%H:%i:%s.%f")
	require.Equal(t, types.T_time, tp)
	require.Equal(t, MaxFsp, fsp)

	tp, fsp = ExtractToDateReturnType("%Y-%%f-%m-%d")
	require.Equal(t, types.T_date, tp)
	require.Equal(t, 0, fsp)
}
