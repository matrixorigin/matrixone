// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package fulltext2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// Exercise the real Preload/Load call sequence, including SQL whose errors are
// deliberately tolerated (build timestamp and generation capture). Empty Base
// and Tail results isolate cancellation at every metadata stage; storage tests
// separately cover nonempty Base streaming and fallback.
func TestSearchOwnerCancelsEveryLoadSQLStage(t *testing.T) {
	for _, preload := range []bool{true, false} {
		name := "Load"
		if preload {
			name = "Preload"
		}
		t.Run(name, func(t *testing.T) {
			var statements []string
			t.Run("trace", func(t *testing.T) {
				sp, _ := mockSqlProcWithIdentity(t, "complete-load")
				owner := newBaseFileOwner(1024, 1)
				s := newFulltext2SearchWithBaseOwner(testStorageCfg(), owner)
				t.Cleanup(func() { s.Destroy(); require.NoError(t, owner.close()) })
				swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
					statements = append(statements, sql)
					return executor.Result{}, nil
				})
				if preload {
					require.NoError(t, s.Preload(sp))
				} else {
					require.NoError(t, s.Load(sp))
				}
			})
			require.NotEmpty(t, statements)
			for target, sql := range statements {
				t.Run(fmt.Sprintf("read-%d", target), func(t *testing.T) {
					t.Log(sql)
					sp, _ := mockSqlProcWithIdentity(t, "complete-load")
					ctx, cancel := context.WithCancel(sp.GetTopContext())
					sp = sp.WithContext(ctx)
					owner := newBaseFileOwner(1024, 1)
					s := newFulltext2SearchWithBaseOwner(testStorageCfg(), owner)
					entered, exited, rescue := make(chan struct{}), make(chan struct{}), make(chan struct{})
					done := make(chan error, 1)
					n := 0
					swapRunSql(t, func(got *sqlexec.SqlProcess, _ string) (executor.Result, error) {
						i := n
						n++
						if i != target {
							return executor.Result{}, nil
						}
						close(entered)
						select {
						case <-got.GetTopContext().Done():
							return executor.Result{}, got.GetTopContext().Err()
						case <-rescue:
							return executor.Result{}, context.Canceled
						}
					})
					// Rescue is independent of both owner and propagated context.
					// Join before restoring package SQL indirections.
					t.Cleanup(func() {
						cancel()
						close(rescue)
						<-exited
						s.Destroy()
						require.NoError(t, owner.close())
					})
					go func() {
						defer close(exited)
						if preload {
							done <- s.Preload(sp)
						} else {
							done <- s.Load(sp)
						}
					}()
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("target SQL not reached")
					}
					owner.beginClose()
					select {
					case err := <-done:
						require.ErrorIs(t, err, context.Canceled)
					case <-time.After(5 * time.Second):
						t.Fatal("shutdown did not cancel complete load")
					}
					<-exited
					require.False(t, s.loaded)
					require.False(t, s.preloaded)
					require.Nil(t, s.idx)
					require.NoError(t, owner.close())
				})
			}
		})
	}
}
