// Copyright 2024 Matrix Origin
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

package merge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeSettings(t *testing.T) {
	settings := DefaultMergeSettings.Clone()
	settings.L0MaxCountDecayControl = []float64{0.1, 0.2, 0.3, 0.4}
	t.Log(settings.String())
	trigger, err := settings.Clone().ToMMsgTaskTrigger()
	require.NoError(t, err)
	require.Equal(t, [4]float64{0.1, 0.2, 0.3, 0.4}, trigger.l0.CPoints)

	{
		settings := DefaultMergeSettings.Clone()
		settings.TombstoneL1Size = "100xx"
		_, err = settings.ToMMsgTaskTrigger()
		require.Error(t, err)
		settings.TombstoneL1Size = "100b"
		_, err = settings.ToMMsgTaskTrigger()
		require.NoError(t, err)

		settings.VacuumScoreDecayDuration = "bad2m1s"
		_, err = settings.ToMMsgTaskTrigger()
		require.Error(t, err)

		settings.VacuumScoreDecayDuration = "2m1s"
		_, err = settings.ToMMsgTaskTrigger()
		require.NoError(t, err)

		settings.L0MaxCountDecayDuration = "bad2m1s"
		_, err = settings.ToMMsgTaskTrigger()
		require.Error(t, err)

		settings.L0MaxCountDecayDuration = "2m1s"
		_, err = settings.ToMMsgTaskTrigger()
		require.NoError(t, err)
	}
}
