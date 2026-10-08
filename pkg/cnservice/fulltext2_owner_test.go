// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cnservice

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFulltext2OwnerCloseRetryPreservesWithdrawalDiagnostic(t *testing.T) {
	diagnostic := errors.New("remote withdrawal diagnostic")
	s := &service{cfg: &Config{}, closeErr: diagnostic, fulltext2BaseReuseClosePending: true}
	// Local teardown has already completed; only owner cleanup was pending.
	s.closeOnce.Do(func() {})
	require.ErrorIs(t, s.Close(), diagnostic)
	require.True(t, s.CloseComplete(), "successful owner retry certifies local teardown despite a remote diagnostic")
	require.False(t, s.fulltext2BaseReuseClosePending)
	require.ErrorIs(t, s.Close(), diagnostic)
	require.True(t, s.CloseComplete())
}

func TestFulltext2OwnerCloseDoesNotCertifyFailedDrain(t *testing.T) {
	failure := errors.New("local producer drain failed")
	s := &service{closeErr: failure}
	s.closeOnce.Do(func() {})
	require.ErrorIs(t, s.Close(), failure)
	require.False(t, s.CloseComplete(), "a nil experimental owner cannot erase a local drain failure")
}
