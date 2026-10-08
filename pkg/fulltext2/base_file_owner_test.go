// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package fulltext2

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBaseFileOwnerSurvivesBorrowerRelease(t *testing.T) {
	owner := newBaseFileOwner(64, 2)
	t.Cleanup(func() { _ = owner.close() })
	pool, err := owner.poolForSearch()
	require.NoError(t, err)
	var fills atomic.Int32
	key := testPoolKey("owner", 8, "abcdefgh")
	borrow := func() error {
		_, err := owner.run(context.Background(), func(context.Context) (*Segment, error) {
			lease, err := pool.acquire(context.Background(), key, testPoolFill(t, &fills, "abcdefgh"))
			if err != nil {
				return nil, err
			}
			lease.Release()
			return nil, nil
		})
		return err
	}
	require.NoError(t, borrow())
	require.NoError(t, borrow(), "a second Search handle may borrow the same owner")
	require.Equal(t, int32(1), fills.Load())
	require.NoError(t, owner.close())
	_, err = owner.poolForSearch()
	require.ErrorIs(t, err, errBaseFileOwnerClosed)
}

func TestBaseFileOwnerCloseWaitsForAdmittedOperation(t *testing.T) {
	owner := newBaseFileOwner(64, 2)
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = owner.close()
	})
	result := make(chan error, 1)
	go func() {
		_, err := owner.run(context.Background(), func(context.Context) (*Segment, error) {
			close(started)
			<-release
			return nil, nil
		})
		result <- err
	}()
	<-started
	closed := make(chan error, 1)
	go func() { closed <- owner.close() }()
	select {
	case err := <-closed:
		t.Fatalf("owner close returned before admitted operation ended: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-result)
	require.NoError(t, <-closed)
	_, err := owner.poolForSearch()
	require.ErrorIs(t, err, errBaseFileOwnerClosed)
}

func TestBaseFileOwnerRejectsOperationAfterClose(t *testing.T) {
	owner := newBaseFileOwner(64, 2)
	t.Cleanup(func() { _ = owner.close() })
	require.NoError(t, owner.close())
	_, err := owner.run(context.Background(), func(context.Context) (*Segment, error) {
		t.Fatal("closed owner must not invoke an operation")
		return nil, nil
	})
	require.ErrorIs(t, err, errBaseFileOwnerClosed)
}

func TestBaseFileOwnerReportsPinnedResourcesUntilRelease(t *testing.T) {
	owner := newBaseFileOwner(64, 1)
	pool, err := owner.poolForSearch()
	require.NoError(t, err)
	lease, err := pool.acquire(context.Background(), testPoolKey("pinned-owner", 8, "abcdefgh"), testPoolFill(t, new(atomic.Int32), "abcdefgh"))
	require.NoError(t, err)
	require.ErrorIs(t, owner.close(), errBaseFileOwnerPending)
	lease.Release()
	require.NoError(t, owner.close())
}
