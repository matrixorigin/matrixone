// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package v2

import (
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestTxnLoadLogtailReadBarrierMetricHasFixedOutcomes(t *testing.T) {
	TxnLoadLogtailReadBarrierSuccessDurationHistogram.Observe(0.001)
	TxnLoadLogtailReadBarrierCanceledDurationHistogram.Observe(0.001)
	TxnLoadLogtailReadBarrierTimeoutDurationHistogram.Observe(0.001)
	TxnLoadLogtailReadBarrierErrorDurationHistogram.Observe(0.001)

	registry := prometheus.NewRegistry()
	registry.MustRegister(txnLoadLogtailReadBarrierDurationHistogram)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Equal(t, "mo_txn_load_logtail_read_barrier_duration_seconds", families[0].GetName())
	require.Len(t, families[0].Metric, 4)
	outcomes := make([]string, 0, 4)
	for _, metric := range families[0].Metric {
		require.Len(t, metric.Label, 1)
		require.Equal(t, "outcome", metric.Label[0].GetName())
		outcomes = append(outcomes, metric.Label[0].GetValue())
	}
	sort.Strings(outcomes)
	require.Equal(t, []string{"canceled", "error", "success", "timeout"}, outcomes)
}
