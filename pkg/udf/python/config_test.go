// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientConfigRequiresExplicitUnisolatedOptIn(t *testing.T) {
	require.NoError(t, (&ClientConfig{}).Validate())
	require.NoError(t, (&ClientConfig{ServerAddress: "127.0.0.1:50051"}).Validate())

	config := &ClientConfig{Enabled: true, ServerAddress: "127.0.0.1:50051"}
	require.ErrorContains(t, config.Validate(), "allow-unisolated")

	config.AllowUnisolated = true
	config.RequestTimeout = time.Second
	require.NoError(t, config.Validate())
}
