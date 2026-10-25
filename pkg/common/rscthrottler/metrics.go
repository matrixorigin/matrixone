// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package rscthrottler

import (
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	registry := metric.GetPrometheusRegistry()
	for kind, read := range map[string]func() float64{
		"retained_data": func() float64 { return float64(processMemoryCoordinator.dataReserved.Load()) },
		"input_promise": func() float64 { return float64(processMemoryCoordinator.inputPromise.Load()) },
		"io_promise":    func() float64 { return float64(processMemoryCoordinator.workspacePromise.Load()) },
		"sample_rss": func() float64 {
			c := processMemoryCoordinator
			c.sampleMu.Lock()
			defer c.sampleMu.Unlock()
			return float64(max(0, c.sample.rss))
		},
		"sample_cgroup_current": func() float64 {
			c := processMemoryCoordinator
			c.sampleMu.Lock()
			defer c.sampleMu.Unlock()
			return float64(max(0, c.sample.current))
		},
		"sample_inactive_file": func() float64 {
			c := processMemoryCoordinator
			c.sampleMu.Lock()
			defer c.sampleMu.Unlock()
			return float64(max(0, c.sample.inactiveFile))
		},
	} {
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "mo", Subsystem: "memory_admission", Name: "bytes", Help: "Memory commitments and overlapping physical samples; physical samples must not be added together.", ConstLabels: prometheus.Labels{"kind": kind}}, read))
	}
	for kind, read := range map[string]func() float64{
		"ordinary": func() float64 {
			c := processMemoryCoordinator
			c.mu.Lock()
			defer c.mu.Unlock()
			return float64(len(c.queue))
		},
		"io": func() float64 {
			c := processMemoryCoordinator
			c.mu.Lock()
			defer c.mu.Unlock()
			return float64(len(c.ioQueue))
		},
		"running_io": func() float64 {
			c := processMemoryCoordinator
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.ioBusy {
				return 1
			}
			return 0
		},
	} {
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "mo", Subsystem: "memory_admission", Name: "tasks", Help: "Memory admission waiters and running IO workspace owners.", ConstLabels: prometheus.Labels{"kind": kind}}, read))
	}
	for kind, read := range map[string]func() float64{
		"sample":      func() float64 { return float64(processMemoryCoordinator.sampleCount.Load()) },
		"reclaim":     func() float64 { return float64(processMemoryCoordinator.reclaimCount.Load()) },
		"native_trim": func() float64 { return float64(processMemoryCoordinator.nativeCount.Load()) },
	} {
		registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "mo", Subsystem: "memory_admission", Name: "events_total", Help: "Actual merged process sampling and reclamation calls.", ConstLabels: prometheus.Labels{"kind": kind}}, read))
	}
}
