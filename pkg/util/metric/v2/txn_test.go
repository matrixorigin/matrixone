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

package v2

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTxnAObjectMaxCommitStateMetric(t *testing.T) {
	const tableID uint64 = 0xfedcba9876543210
	beforeTables := testutil.ToFloat64(TxnAObjectMaxCommitTableTotalGauge)
	beforeSeries := testutil.CollectAndCount(TxnAObjectMaxCommitStateGauge)

	// The metric is a snapshot per table/object-list.  Data and tombstone
	// lists must contribute to one table without double-counting that table.
	UpdateTxnAObjectMaxCommitState(tableID, false, 2, 3, 4)
	UpdateTxnAObjectMaxCommitState(tableID, true, 1, 0, 2)
	require.Equal(t, float64(2), testutil.ToFloat64(
		TxnAObjectMaxCommitStateGauge.WithLabelValues("18364758544493064720", "data", "pending")))
	require.Equal(t, float64(3), testutil.ToFloat64(
		TxnAObjectMaxCommitStateGauge.WithLabelValues("18364758544493064720", "data", "sealed_waiting")))
	require.Equal(t, float64(4), testutil.ToFloat64(
		TxnAObjectMaxCommitStateGauge.WithLabelValues("18364758544493064720", "data", "finalized")))
	require.Equal(t, beforeTables+1, testutil.ToFloat64(TxnAObjectMaxCommitTableTotalGauge))

	// Removing both list snapshots evicts the table and its label children.
	UpdateTxnAObjectMaxCommitState(tableID, false, 0, 0, 0)
	UpdateTxnAObjectMaxCommitState(tableID, true, 0, 0, 0)
	require.Equal(t, beforeTables, testutil.ToFloat64(TxnAObjectMaxCommitTableTotalGauge))
	require.Equal(t, beforeSeries, testutil.CollectAndCount(TxnAObjectMaxCommitStateGauge))
	require.NotContains(t, txnAObjectMetricCounts, tableID)

	// An all-zero first snapshot must not retain a table or create series.
	UpdateTxnAObjectMaxCommitState(tableID, false, 0, 0, 0)
	require.Equal(t, beforeSeries, testutil.CollectAndCount(TxnAObjectMaxCommitStateGauge))
	require.NotContains(t, txnAObjectMetricCounts, tableID)
}

func TestTxnAObjectMetricsEvictDroppedTables(t *testing.T) {
	beforeTables := testutil.ToFloat64(TxnAObjectMaxCommitTableTotalGauge)
	beforeSeries := testutil.CollectAndCount(TxnAObjectMaxCommitStateGauge)
	for i := uint64(0); i < 100; i++ {
		id := uint64(0xface000000000000) + i
		UpdateTxnAObjectMaxCommitState(id, false, 1, 0, 0)
		UpdateTxnAObjectMaxCommitState(id, true, 0, 1, 0)
		RemoveTxnAObjectMaxCommitState(id)
		require.NotContains(t, txnAObjectMetricCounts, id)
	}
	require.Equal(t, beforeTables, testutil.ToFloat64(TxnAObjectMaxCommitTableTotalGauge))
	require.Equal(t, beforeSeries, testutil.CollectAndCount(TxnAObjectMaxCommitStateGauge))
}
