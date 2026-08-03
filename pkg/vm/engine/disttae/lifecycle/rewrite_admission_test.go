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

package lifecycle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRewriteAdmissionAmplificationAndWindowBudgets(t *testing.T) {
	admission, err := NewRewriteAdmission(RewriteReleaseProfile{
		Window:                   time.Hour,
		MaxAmplification:         4,
		MaxSourceBytesPerAccount: 100,
		MaxSourceBytesPerCluster: 150,
	})
	require.NoError(t, err)
	now := time.Unix(3600, 0)
	require.ErrorContains(t, admission.CheckAmplification(50, 10), "rewrite amplification")
	require.NoError(t, admission.CheckAmplification(60, 20))
	require.NoError(t, admission.ReserveSource(1, 60, now))
	require.ErrorContains(t, admission.ReserveSource(1, 60, now), "account Rewrite byte window exhausted")
	require.NoError(t, admission.ReserveSource(2, 90, now))
	require.ErrorContains(t, admission.ReserveSource(3, 1, now), "cluster Rewrite byte window exhausted")
	require.NoError(t, admission.ReserveSource(1, 100, now.Add(time.Hour)))
}

func TestRewriteAdmissionReservesSourceBeforeClassification(t *testing.T) {
	admission, err := NewRewriteAdmission(RewriteReleaseProfile{
		Window:                   time.Hour,
		MaxAmplification:         4,
		MaxSourceBytesPerAccount: 100,
		MaxSourceBytesPerCluster: 150,
	})
	require.NoError(t, err)
	now := time.Unix(3600, 0)
	require.NoError(t, admission.ReserveSource(1, 80, now))
	require.ErrorContains(t,
		admission.ReserveSource(1, 30, now),
		"account Rewrite byte window exhausted",
	)
	require.ErrorContains(t,
		admission.CheckAmplification(90, 10),
		"rewrite amplification",
	)
	require.NoError(t, admission.CheckAmplification(30, 20))
}
