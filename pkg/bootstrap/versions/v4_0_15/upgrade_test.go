// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package v4_0_15

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewUpgrade(t *testing.T) {
	require.Equal(t, "4.0.15", Handler.Metadata().Version)
	require.Equal(t, defines.MORPCVersion112, Handler.Metadata().RequiredProtocolVersion)
	for _, failure := range []bool{false, true} {
		calls := 0
		injected := errors.New("catalog creation failed")
		tx := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			calls++
			require.True(t, strings.HasPrefix(sql, "create cluster table if not exists mo_catalog.mo_"))
			if failure && calls == 2 {
				return executor.Result{}, injected
			}
			return executor.Result{}, nil
		}, nil)
		err := Handler.HandleClusterUpgrade(context.Background(), tx)
		if failure {
			require.ErrorIs(t, err, injected)
			require.Equal(t, 2, calls)
		} else {
			require.NoError(t, err)
			require.Equal(t, 4, calls)
		}
	}
}
