// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");

package runtime

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestProcessInstanceIdentitySharedAndStable(t *testing.T) {
	id := ProcessInstanceID()
	_, err := uuid.Parse(id)
	require.NoError(t, err)
	const callers = 64
	identities := make(chan string, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			identities <- ProcessInstanceID()
		}()
	}
	wg.Wait()
	close(identities)
	for actual := range identities {
		require.Equal(t, id, actual, "CN services share one physical coordinator identity")
	}
}
