// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

//go:build matrixone_mv_faults

package main

import (
	"context"
	"os"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

// Only the explicitly instrumented native recovery binary accepts startup
// faults. Production builds have no environment-controlled injection path.
// Startup injection is needed because ordinary login cannot precede migration.
func init() {
	if point := os.Getenv("MO_MV_STARTUP_FAULT"); point != "" {
		fault.Enable()
		if result := fault.HandleFaultInject(context.Background(), fault.AddFault, point); result != "OK" {
			panic(result)
		}
	}
}
