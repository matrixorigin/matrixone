package cdc

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type watermarkRowFetcher interface {
	GetTableDataByPK(dbName, tableName string, pkValues []string) ([]string, error)
}

func waitForErrorMetadata(t *testing.T, fetcher watermarkRowFetcher, key *WatermarkKey, cond func(string, *ErrorMetadata) bool) (string, *ErrorMetadata) {
	t.Helper()
	var (
		tuple []string
		meta  *ErrorMetadata
		msg   string
		err   error
	)

	require.Eventually(t, func() bool {
		tuple, err = fetcher.GetTableDataByPK(
			"mo_catalog",
			"mo_cdc_watermark",
			[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
		)
		if err != nil || len(tuple) < 5 {
			return false
		}
		msg = strings.Trim(tuple[4], "'")
		meta = ParseErrorMetadata(msg)
		if cond == nil {
			return meta != nil
		}
		return cond(msg, meta)
	}, time.Second, 10*time.Millisecond, "timeout waiting for error metadata")

	require.NoError(t, err)
	require.NotNil(t, meta)

	return msg, meta
}

// TestRetryCountTracking verifies that retry counts are properly tracked and persisted
func TestRetryCountTracking(t *testing.T) {
	u, ie := InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()

	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task1",
		DBName:    "db1",
		TableName: "t1",
	}

	// Initialize watermark
	ts := types.BuildTS(1, 1)
	ret, err := u.GetOrAddCommitted(context.Background(), key, &ts)
	require.NoError(t, err)
	assert.Equal(t, ts, ret)

	// Test 1: First retryable error (retry count = 1)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"connection timeout",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)

	errMsg1, metadata1 := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return meta != nil && meta.IsRetryable && meta.RetryCount >= 1
	})
	assert.True(t, metadata1.IsRetryable, "First error should be retryable")
	assert.Equal(t, 1, metadata1.RetryCount, "First retry count should be 1")
	assert.Contains(t, metadata1.Message, "connection timeout")

	fmt.Printf("Retry 1 OK: %s (count=%d)\n", errMsg1, metadata1.RetryCount)

	// Test 2: Second retryable error (retry count = 2)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"connection timeout again",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)

	errMsg2, metadata2 := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return meta != nil && meta.IsRetryable && meta.RetryCount >= 2
	})
	assert.True(t, metadata2.IsRetryable, "Second error should still be retryable")
	assert.Equal(t, 2, metadata2.RetryCount, "Second retry count should be 2")

	fmt.Printf("Retry 2 OK: %s (count=%d)\n", errMsg2, metadata2.RetryCount)

	// Test 3: Third retryable error (retry count = 3)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"still failing",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)

	errMsg3, metadata3 := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return meta != nil && meta.IsRetryable && meta.RetryCount >= 3
	})
	assert.True(t, metadata3.IsRetryable, "Third error should still be retryable")
	assert.Equal(t, 3, metadata3.RetryCount, "Third retry count should be 3")

	fmt.Printf("Retry 3 OK: %s (count=%d)\n", errMsg3, metadata3.RetryCount)

	// Test 4: Fourth retryable error (count = 4, exceeds MaxRetryCount=3, should convert to non-retryable)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"exceeded max retries",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)

	errMsg4, metadata4 := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return (meta != nil && !meta.IsRetryable && meta.RetryCount >= 4) || strings.Contains(raw, "max retry exceeded")
	})
	assert.False(t, metadata4.IsRetryable, "Fourth error should be converted to non-retryable")
	assert.Contains(t, metadata4.Message, "max retry exceeded")

	fmt.Printf("Retry 4 OK (converted): %s (retryable=%v)\n", errMsg4, metadata4.IsRetryable)
}

// TestNonRetryableErrorNoCount verifies non-retryable errors don't track retry count
func TestNonRetryableErrorNoCount(t *testing.T) {
	u, ie := InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()

	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task2",
		DBName:    "db2",
		TableName: "t2",
	}

	// Initialize watermark
	ts := types.BuildTS(1, 1)
	_, err := u.GetOrAddCommitted(context.Background(), key, &ts)
	require.NoError(t, err)

	// Non-retryable error (should not track retry count)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"type mismatch error",
		&ErrorContext{IsRetryable: false},
	)
	require.NoError(t, err)

	errMsg, metadata := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return meta != nil && !meta.IsRetryable
	})
	assert.False(t, metadata.IsRetryable)
	assert.Equal(t, 0, metadata.RetryCount, "Non-retryable errors don't track retry count")

	fmt.Printf("Non-retryable OK: %s (count=%d)\n", errMsg, metadata.RetryCount)

	// Multiple non-retryable errors (retry count should stay 0)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"another non-retryable error",
		&ErrorContext{IsRetryable: false},
	)
	require.NoError(t, err)

	errMsg2, metadata2 := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return meta != nil && !meta.IsRetryable
	})
	assert.False(t, metadata2.IsRetryable)
	assert.Equal(t, 0, metadata2.RetryCount, "Non-retryable errors still don't track retry count")

	fmt.Printf("Non-retryable OK (again): %s (count=%d)\n", errMsg2, metadata2.RetryCount)
}

// TestErrorTypeChange verifies retry count resets when error type changes
func TestErrorTypeChange(t *testing.T) {
	u, ie := InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()

	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task3",
		DBName:    "db3",
		TableName: "t3",
	}

	// Initialize watermark
	ts := types.BuildTS(1, 1)
	_, err := u.GetOrAddCommitted(context.Background(), key, &ts)
	require.NoError(t, err)

	// Start with retryable error (count = 1)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"retryable error 1",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)
	_, metadata1 := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return meta != nil && meta.IsRetryable && meta.RetryCount >= 1
	})
	assert.Equal(t, 1, metadata1.RetryCount)

	// Another retryable error (count = 2)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"retryable error 2",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)
	_, metadata2 := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return meta != nil && meta.IsRetryable && meta.RetryCount >= 2
	})
	assert.Equal(t, 2, metadata2.RetryCount)

	fmt.Printf("Before type change: retryable (count=%d)\n", metadata2.RetryCount)

	// Change to non-retryable error (count should reset to 0)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"non-retryable error",
		&ErrorContext{IsRetryable: false},
	)
	require.NoError(t, err)
	_, metadata3 := waitForErrorMetadata(t, ie, key, func(raw string, meta *ErrorMetadata) bool {
		return meta != nil && !meta.IsRetryable
	})
	assert.False(t, metadata3.IsRetryable)
	assert.Equal(t, 0, metadata3.RetryCount, "Count resets when error type changes")

	fmt.Printf("After type change: non-retryable (count=%d)\n", metadata3.RetryCount)
}
