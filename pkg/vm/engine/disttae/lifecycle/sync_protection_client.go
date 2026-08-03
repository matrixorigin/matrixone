// Copyright 2026 Matrix Origin
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

package lifecycle

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// SQLSyncProtectionClient reuses MO's existing disk-cleaner control command.
// The command is broadcast to TNs by mo_ctl; Lifecycle adds no GC registry.
type SQLSyncProtectionClient struct {
	Executor    executor.SQLExecutor
	FileService fileservice.FileService
	TaskID      string
}

func (client SQLSyncProtectionClient) Register(
	ctx context.Context,
	jobID string,
	objects []objectio.ObjectStats,
	validUntil time.Time,
) error {
	filter, err := buildLifecycleSyncProtectionFilter(objects)
	if err != nil {
		return err
	}
	request := map[string]any{
		"job_id":   jobID,
		"bf":       filter,
		"valid_ts": validUntil.UnixNano(),
		"task_id":  client.TaskID,
	}
	return client.exec(ctx, "register_sync_protection", request)
}

func (client SQLSyncProtectionClient) StatExact(
	ctx context.Context,
	objects []objectio.ObjectStats,
) error {
	if client.FileService == nil {
		return moerr.NewInternalErrorNoCtxf("Lifecycle SyncProtection FileService is nil")
	}
	for _, object := range objects {
		location := object.ObjectLocation()
		if location.IsEmpty() {
			return moerr.NewInternalErrorNoCtxf("Lifecycle protected Object has no exact location")
		}
		entry, err := client.FileService.StatFile(ctx, location.Name().String())
		if err != nil {
			return err
		}
		if entry.Size != int64(object.Size()) {
			return moerr.NewInternalErrorNoCtxf(
				"Lifecycle protected Object %s size changed",
				location.Name().String(),
			)
		}
	}
	return nil
}

func (client SQLSyncProtectionClient) Renew(
	ctx context.Context,
	jobID string,
	validUntil time.Time,
) error {
	return client.exec(ctx, "renew_sync_protection", map[string]any{
		"job_id":   jobID,
		"valid_ts": validUntil.UnixNano(),
	})
}

func (client SQLSyncProtectionClient) Release(
	ctx context.Context,
	jobID string,
) error {
	return client.exec(ctx, "unregister_sync_protection", map[string]any{
		"job_id": jobID,
	})
}

func (client SQLSyncProtectionClient) exec(
	ctx context.Context,
	operation string,
	request map[string]any,
) error {
	if client.Executor == nil {
		return moerr.NewInternalErrorNoCtxf("Lifecycle SyncProtection SQL executor is nil")
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		"select mo_ctl('dn','diskcleaner','%s.%s')",
		operation,
		strings.ReplaceAll(string(encoded), "'", "''"),
	)
	result, err := client.Executor.Exec(
		ctx,
		sql,
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return err
	}
	defer result.Close()
	var responses int
	var responseErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 1 {
			responseErr = moerr.NewInternalErrorNoCtxf("Lifecycle SyncProtection response has %d columns", len(columns))
			return false
		}
		for row := 0; row < rows; row++ {
			responses++
			if err := validateLifecycleMoCtlResponse(columns[0].GetStringAt(row)); err != nil {
				responseErr = err
				return false
			}
		}
		return true
	})
	if responseErr != nil {
		return responseErr
	}
	if responses == 0 {
		return moerr.NewInternalErrorNoCtxf("Lifecycle SyncProtection command returned no TN response")
	}
	return nil
}

func buildLifecycleSyncProtectionFilter(
	objects []objectio.ObjectStats,
) (string, error) {
	if len(objects) == 0 {
		return "", moerr.NewInternalErrorNoCtxf("Lifecycle SyncProtection Object set is empty")
	}
	// SyncProtectionManager decodes pkg/vm/engine/tae/index.BloomFilter.
	// Produce that exact existing wire format rather than introducing a
	// Lifecycle-specific filter codec.
	values := containers.MakeVector(
		types.T_varchar.ToType(),
		common.DefaultAllocator,
	)
	defer values.Close()
	for _, object := range objects {
		if object.IsZero() {
			return "", moerr.NewInternalErrorNoCtxf("Lifecycle SyncProtection Object identity is empty")
		}
		values.Append([]byte(object.ObjectName().String()), false)
	}
	filter, err := index.NewBloomFilter(values, nil, nil, nil)
	if err != nil {
		return "", err
	}
	encoded, err := filter.Marshal()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(encoded), nil
}

func validateLifecycleMoCtlResponse(encoded string) error {
	var outer struct {
		Result []struct {
			ReturnString string `json:"ReturnStr"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(encoded), &outer); err != nil {
		return err
	}
	if len(outer.Result) == 0 {
		return moerr.NewInternalErrorNoCtxf("Lifecycle SyncProtection response has no result")
	}
	for _, item := range outer.Result {
		var inner struct {
			Status  string `json:"status"`
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal([]byte(item.ReturnString), &inner); err != nil {
			return err
		}
		if inner.Status != "ok" {
			return moerr.NewInternalErrorNoCtxf(
				"Lifecycle SyncProtection failed: %s: %s",
				inner.Code,
				inner.Message,
			)
		}
	}
	return nil
}
