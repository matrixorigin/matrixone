package cdc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	time.Sleep(50 * time.Millisecond) // Wait for async flush

	tuple, err := ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)
	require.Len(t, tuple, 5)

	errMsg1 := tuple[4]
	metadata1 := ParseErrorMetadata(errMsg1)
	require.NotNil(t, metadata1)
	assert.True(t, metadata1.IsRetryable, "First error should be retryable")
	assert.Equal(t, 1, metadata1.RetryCount, "First retry count should be 1")
	assert.Contains(t, metadata1.Message, "connection timeout")

	fmt.Printf("✅ Retry 1: %s (count=%d)\n", errMsg1, metadata1.RetryCount)

	// Test 2: Second retryable error (retry count = 2)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"connection timeout again",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	tuple, err = ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)

	errMsg2 := tuple[4]
	metadata2 := ParseErrorMetadata(errMsg2)
	require.NotNil(t, metadata2)
	assert.True(t, metadata2.IsRetryable, "Second error should still be retryable")
	assert.Equal(t, 2, metadata2.RetryCount, "Second retry count should be 2")

	fmt.Printf("✅ Retry 2: %s (count=%d)\n", errMsg2, metadata2.RetryCount)

	// Test 3: Third retryable error (retry count = 3)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"still failing",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	tuple, err = ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)

	errMsg3 := tuple[4]
	metadata3 := ParseErrorMetadata(errMsg3)
	require.NotNil(t, metadata3)
	assert.True(t, metadata3.IsRetryable, "Third error should still be retryable")
	assert.Equal(t, 3, metadata3.RetryCount, "Third retry count should be 3")

	fmt.Printf("✅ Retry 3: %s (count=%d)\n", errMsg3, metadata3.RetryCount)

	// Test 4: Fourth retryable error (count = 4, exceeds MaxRetryCount=3, should convert to non-retryable)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"exceeded max retries",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	tuple, err = ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)

	errMsg4 := tuple[4]
	metadata4 := ParseErrorMetadata(errMsg4)
	require.NotNil(t, metadata4)
	assert.False(t, metadata4.IsRetryable, "Fourth error should be converted to non-retryable")
	assert.Contains(t, metadata4.Message, "max retry exceeded")

	fmt.Printf("✅ Retry 4 (converted): %s (retryable=%v)\n", errMsg4, metadata4.IsRetryable)
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

	time.Sleep(50 * time.Millisecond)

	tuple, err := ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)

	errMsg := tuple[4]
	metadata := ParseErrorMetadata(errMsg)
	require.NotNil(t, metadata)
	assert.False(t, metadata.IsRetryable)
	assert.Equal(t, 0, metadata.RetryCount, "Non-retryable errors don't track retry count")

	fmt.Printf("✅ Non-retryable: %s (count=%d)\n", errMsg, metadata.RetryCount)

	// Multiple non-retryable errors (retry count should stay 0)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"another non-retryable error",
		&ErrorContext{IsRetryable: false},
	)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	tuple, err = ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)

	errMsg2 := tuple[4]
	metadata2 := ParseErrorMetadata(errMsg2)
	require.NotNil(t, metadata2)
	assert.False(t, metadata2.IsRetryable)
	assert.Equal(t, 0, metadata2.RetryCount, "Non-retryable errors still don't track retry count")

	fmt.Printf("✅ Non-retryable (again): %s (count=%d)\n", errMsg2, metadata2.RetryCount)
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
	time.Sleep(50 * time.Millisecond)

	tuple, err := ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)
	metadata1 := ParseErrorMetadata(tuple[4])
	assert.Equal(t, 1, metadata1.RetryCount)

	// Another retryable error (count = 2)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"retryable error 2",
		&ErrorContext{IsRetryable: true},
	)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	tuple, err = ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)
	metadata2 := ParseErrorMetadata(tuple[4])
	assert.Equal(t, 2, metadata2.RetryCount)

	fmt.Printf("✅ Before type change: retryable (count=%d)\n", metadata2.RetryCount)

	// Change to non-retryable error (count should reset to 0)
	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"non-retryable error",
		&ErrorContext{IsRetryable: false},
	)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	tuple, err = ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	require.NoError(t, err)
	metadata3 := ParseErrorMetadata(tuple[4])
	assert.False(t, metadata3.IsRetryable)
	assert.Equal(t, 0, metadata3.RetryCount, "Count resets when error type changes")

	fmt.Printf("✅ After type change: non-retryable (count=%d)\n", metadata3.RetryCount)
}
