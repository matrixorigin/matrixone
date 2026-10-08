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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalLocaleResolution(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "lc_time_names" {
			return "fr_FR", nil
		}
		return "", nil
	})

	require.Equal(t, "dimanche", temporalLocaleForProcess(proc).localizedWeekday(0))
	require.Equal(t, "décembre", temporalLocaleForProcess(proc).localizedMonth(12))
	require.Equal(t, "déc", temporalLocaleForProcess(proc).localizedMonthAbbrev(12))
	require.Equal(t, "jun", temporalLocaleForProcess(proc).localizedMonthAbbrev(6))
	require.Equal(t, "jui", temporalLocaleForProcess(proc).localizedMonthAbbrev(7))
}

func TestTemporalLocaleResolutionFromRemoteSessionSnapshot(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(nil)
	proc.GetSessionInfo().LCTimeNames = "fr_FR"

	require.Equal(t, "dimanche", temporalLocaleForProcess(proc).localizedWeekday(0))
	require.Equal(t, "décembre", temporalLocaleForProcess(proc).localizedMonth(12))
}

func TestTemporalLocaleFallbackAndBounds(t *testing.T) {
	require.Equal(t, "Sunday", temporalLocaleForProcess(nil).localizedWeekday(0))
	require.Equal(t, "", temporalLocaleForProcess(nil).localizedWeekday(-1))
	require.Equal(t, "", temporalLocaleForProcess(nil).localizedMonth(13))
}
