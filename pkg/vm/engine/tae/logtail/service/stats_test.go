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

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerStats(t *testing.T) {
	stats := newServerStatistics()

	stats.subscriptionRequestInc()
	require.Equal(t, uint64(1), stats.subscriptionRequests())

	stats.unsubscriptionRequestInc()
	require.Equal(t, uint64(1), stats.unsubscriptionRequests())

	t.Log(stats.Stats())
}

func TestSessionStats(t *testing.T) {
	stats := newSessionStatistics()

	stats.bufferGaugeInc()
	stats.bufferGaugeInc()
	stats.bufferGaugeDec()
	require.Equal(t, int64(1), stats.buffers())

	stats.tableGaugeInc()
	stats.tableGaugeInc()
	stats.tableGaugeDec()
	require.Equal(t, int32(1), stats.tables())

	stats.subscriptionResponseInc()
	require.Equal(t, uint64(1), stats.subscriptionResponses())

	stats.unsubscriptionResponseInc()
	require.Equal(t, uint64(1), stats.unsubscriptionResponses())

	stats.updateResponseInc()
	require.Equal(t, uint64(1), stats.updateResponses())

	stats.errorResponseInc()
	require.Equal(t, uint64(1), stats.errorResponses())

	t.Log(stats.Stats())
}
