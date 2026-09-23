// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package v2

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTxnAObjectMaxCommitStateMetric(t *testing.T) {
	const tableID uint64 = 0xfedcba9876543210
	beforeTables := testutil.ToFloat64(TxnAObjectMaxCommitTableTotalGauge)

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

	// Removing both list snapshots removes the table from the total while
	// leaving the per-label Prometheus series at zero.
	UpdateTxnAObjectMaxCommitState(tableID, false, 0, 0, 0)
	UpdateTxnAObjectMaxCommitState(tableID, true, 0, 0, 0)
	require.Equal(t, beforeTables, testutil.ToFloat64(TxnAObjectMaxCommitTableTotalGauge))
}
