// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package publication

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	// SyncProtectionTTLDuration is the TTL duration for sync protection
	SyncProtectionTTLDuration = 15 * time.Minute

	// SyncProtectionRenewInterval is the interval for renewing sync protection
	SyncProtectionRenewInterval = 5 * time.Minute
)

// RegisterSyncProtectionOnDownstreamFn is a function variable that can be stubbed in tests
var RegisterSyncProtectionOnDownstreamFn = func(
	ctx context.Context,
	downstreamExecutor SQLExecutor,
	objectMap map[objectio.ObjectId]*ObjectWithTableInfo,
	mp *mpool.MPool,
) (jobID string, ttlExpireTS int64, retryable bool, err error) {
	return registerSyncProtectionOnDownstreamImpl(ctx, downstreamExecutor, objectMap, mp)
}

const (

	// BloomFilterExpectedItems is the expected number of items in bloom filter
	BloomFilterExpectedItems = 100000

	// BloomFilterFalsePositiveRate is the false positive rate for bloom filter
	BloomFilterFalsePositiveRate = 0.01
)

// GCStatus represents the response from gc_status mo_ctl command
type GCStatus struct {
	Running     bool  `json:"running"`
	Protections int   `json:"protections"`
	TS          int64 `json:"ts"`
}

// SyncProtectionResponse represents the response from sync protection mo_ctl commands
type SyncProtectionResponse struct {
	Status  string `json:"status"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// QueryGCStatus queries the GC status from upstream
func QueryGCStatus(ctx context.Context, executor SQLExecutor) (*GCStatus, error) {
	sql := PublicationSQLBuilder.GCStatusSQL()

	result, cancel, err := executor.ExecSQL(ctx, nil, InvalidAccountID, sql, false, true, time.Minute)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to query GC status: %v", err)
	}
	defer func() {
		if result != nil {
			result.Close()
		}
		if cancel != nil {
			cancel()
		}
	}()

	var responseJSON string
	if !result.Next() {
		if err := result.Err(); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read GC status result: %v", err)
		}
		return nil, moerr.NewInternalErrorNoCtx("no rows returned for GC status query")
	}

	if err := result.Scan(&responseJSON); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to scan GC status result: %v", err)
	}

	if responseJSON == "" {
		return nil, moerr.NewInternalErrorNoCtx("GC status response is empty")
	}

	var status GCStatus
	if err := json.Unmarshal([]byte(responseJSON), &status); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to parse GC status response: %v", err)
	}

	return &status, nil
}

// RegisterSyncProtection registers a sync protection with the upstream GC
func RegisterSyncProtection(
	ctx context.Context,
	executor SQLExecutor,
	jobID string,
	bfBase64 string,
	gcTS int64,
	ttlExpireTS int64,
) error {
	sql := PublicationSQLBuilder.RegisterSyncProtectionSQL(jobID, bfBase64, gcTS, ttlExpireTS)

	result, cancel, err := executor.ExecSQL(ctx, nil, InvalidAccountID, sql, false, true, time.Minute)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to register sync protection: %v", err)
	}
	defer func() {
		if result != nil {
			result.Close()
		}
		if cancel != nil {
			cancel()
		}
	}()

	var responseJSON string
	if !result.Next() {
		if err := result.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to read register sync protection result: %v", err)
		}
		return moerr.NewInternalErrorNoCtx("no rows returned for register sync protection")
	}

	if err := result.Scan(&responseJSON); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to scan register sync protection result: %v", err)
	}

	if responseJSON == "" {
		return moerr.NewInternalErrorNoCtx("register sync protection response is empty")
	}

	var response SyncProtectionResponse
	if err := json.Unmarshal([]byte(responseJSON), &response); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to parse register sync protection response: %v", err)
	}

	if response.Status != "ok" {
		// Return error with code for retry logic
		if response.Code == "ErrGCRunning" {
			return moerr.NewInternalErrorf(ctx, "ErrGCRunning: %s", response.Message)
		}
		return moerr.NewInternalErrorf(ctx, "register sync protection failed: %s - %s", response.Code, response.Message)
	}

	return nil
}

// RenewSyncProtection renews the TTL of a sync protection
func RenewSyncProtection(
	ctx context.Context,
	executor SQLExecutor,
	jobID string,
	ttlExpireTS int64,
) error {
	sql := PublicationSQLBuilder.RenewSyncProtectionSQL(jobID, ttlExpireTS)

	result, cancel, err := executor.ExecSQL(ctx, nil, InvalidAccountID, sql, false, true, time.Minute)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to renew sync protection: %v", err)
	}
	defer func() {
		if result != nil {
			result.Close()
		}
		if cancel != nil {
			cancel()
		}
	}()

	var responseJSON string
	if !result.Next() {
		if err := result.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to read renew sync protection result: %v", err)
		}
		return moerr.NewInternalErrorNoCtx("no rows returned for renew sync protection")
	}

	if err := result.Scan(&responseJSON); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to scan renew sync protection result: %v", err)
	}

	if responseJSON == "" {
		return moerr.NewInternalErrorNoCtx("renew sync protection response is empty")
	}

	var response SyncProtectionResponse
	if err := json.Unmarshal([]byte(responseJSON), &response); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to parse renew sync protection response: %v", err)
	}

	if response.Status != "ok" {
		return moerr.NewInternalErrorf(ctx, "renew sync protection failed: %s - %s", response.Code, response.Message)
	}

	return nil
}

// UnregisterSyncProtection unregisters a sync protection
func UnregisterSyncProtection(
	ctx context.Context,
	executor SQLExecutor,
	jobID string,
) error {
	sql := PublicationSQLBuilder.UnregisterSyncProtectionSQL(jobID)

	result, cancel, err := executor.ExecSQL(ctx, nil, InvalidAccountID, sql, false, true, time.Minute)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to unregister sync protection: %v", err)
	}
	defer func() {
		if result != nil {
			result.Close()
		}
		if cancel != nil {
			cancel()
		}
	}()

	var responseJSON string
	if !result.Next() {
		if err := result.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to read unregister sync protection result: %v", err)
		}
		return moerr.NewInternalErrorNoCtx("no rows returned for unregister sync protection")
	}

	if err := result.Scan(&responseJSON); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to scan unregister sync protection result: %v", err)
	}

	// Unregister always succeeds even if job doesn't exist
	return nil
}

// BuildBloomFilterFromObjectMap builds a bloom filter from the object map using object name strings
func BuildBloomFilterFromObjectMap(objectMap map[objectio.ObjectId]*ObjectWithTableInfo, mp *mpool.MPool) (string, error) {
	if len(objectMap) == 0 {
		return "", nil
	}

	// Create bloom filter with appropriate size
	expectedItems := len(objectMap)
	if expectedItems < BloomFilterExpectedItems {
		expectedItems = BloomFilterExpectedItems
	}

	bf := bloomfilter.New(int64(expectedItems), BloomFilterFalsePositiveRate)

	// Add all object names (strings) to bloom filter
	vec := vector.NewVec(types.T_varchar.ToType())
	for objID := range objectMap {
		// Convert ObjectId to ObjectName string
		objName := objectio.BuildObjectNameWithObjectID(&objID).String()
		if err := vector.AppendBytes(vec, []byte(objName), false, mp); err != nil {
			vec.Free(mp)
			return "", moerr.NewInternalErrorf(context.Background(), "failed to append to vector: %v", err)
		}
	}
	bf.Add(vec)
	vec.Free(mp)

	// Marshal and encode to base64
	bfBytes, err := bf.Marshal()
	if err != nil {
		return "", moerr.NewInternalErrorf(context.Background(), "failed to marshal bloom filter: %v", err)
	}

	return base64.StdEncoding.EncodeToString(bfBytes), nil
}

// IsGCRunningError checks if the error is a GC running error
func IsGCRunningError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "GC is running")
}

// IsSyncProtectionNotFoundError checks if the error indicates sync protection not found
func IsSyncProtectionNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "sync protection not found")
}

// IsSyncProtectionExistsError checks if the error indicates sync protection already exists
func IsSyncProtectionExistsError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "sync protection already exists")
}

// IsSyncProtectionMaxCountError checks if the error indicates max count reached
func IsSyncProtectionMaxCountError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "sync protection max count reached")
}

// IsSyncProtectionSoftDeleteError checks if the error indicates sync protection is soft deleted
func IsSyncProtectionSoftDeleteError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "sync protection is soft deleted")
}

// IsSyncProtectionInvalidError checks if the error indicates invalid sync protection request
func IsSyncProtectionInvalidError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "invalid sync protection request")
}

// RegisterSyncProtectionOnDownstream registers sync protection on the downstream cluster
// It generates a new UUID for the jobID and sends all commands to downstream
// No retry is performed here - executor already has retry mechanism
// Returns retryable=true if the error is retriable (GC running, max count reached)
func RegisterSyncProtectionOnDownstream(
	ctx context.Context,
	downstreamExecutor SQLExecutor,
	objectMap map[objectio.ObjectId]*ObjectWithTableInfo,
	mp *mpool.MPool,
) (jobID string, ttlExpireTS int64, retryable bool, err error) {
	return RegisterSyncProtectionOnDownstreamFn(ctx, downstreamExecutor, objectMap, mp)
}

// registerSyncProtectionOnDownstreamImpl is the actual implementation
func registerSyncProtectionOnDownstreamImpl(
	ctx context.Context,
	downstreamExecutor SQLExecutor,
	objectMap map[objectio.ObjectId]*ObjectWithTableInfo,
	mp *mpool.MPool,
) (jobID string, ttlExpireTS int64, retryable bool, err error) {
	// Generate a new UUID for job ID
	jobID = uuid.New().String()

	// 1. Query GC status on downstream
	gcStatus, err := QueryGCStatus(ctx, downstreamExecutor)
	if err != nil {
		retryable = IsGCRunningError(err) || IsSyncProtectionMaxCountError(err)
		return "", 0, retryable, moerr.NewInternalErrorf(ctx, "failed to query GC status on downstream: %v", err)
	}

	if gcStatus.Running {
		return "", 0, true, moerr.NewInternalErrorNoCtx("GC is running on downstream, please retry later")
	}

	// 2. Build Bloom Filter
	bfBase64, err := BuildBloomFilterFromObjectMap(objectMap, mp)
	if err != nil {
		return "", 0, false, moerr.NewInternalErrorf(ctx, "failed to build bloom filter: %v", err)
	}

	// 3. Register protection on downstream
	ttlExpireTS = time.Now().Add(SyncProtectionTTLDuration).UnixNano()
	err = RegisterSyncProtection(ctx, downstreamExecutor, jobID, bfBase64, gcStatus.TS, ttlExpireTS)
	if err != nil {
		retryable = IsGCRunningError(err) || IsSyncProtectionMaxCountError(err)
		return "", 0, retryable, moerr.NewInternalErrorf(ctx, "failed to register sync protection on downstream: %v", err)
	}

	return jobID, ttlExpireTS, false, nil
}
