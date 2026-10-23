// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/stretchr/testify/require"
)

func TestImportMemoryCapacityDoesNotForceWholeTransactionRollback(t *testing.T) {
	err := &rscthrottler.WorkingSetCapacityError{Minimum: 300, Available: 200}
	require.False(t, isErrorRollbackWholeTxn(err), "the existing statement rollback path must preserve prior successful explicit transaction statements")
}
