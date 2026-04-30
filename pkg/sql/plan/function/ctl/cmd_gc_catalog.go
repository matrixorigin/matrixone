// Copyright 2024 Matrix Origin
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

package ctl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// handleGCCatalogCache triggers an immediate GC of the CN catalog cache.
//
// Usage:
//
//	select mo_ctl('cn', 'GCCatalogCache', '');       -- default: GC entries older than 20 minutes
//	select mo_ctl('cn', 'GCCatalogCache', '5');      -- GC entries older than 5 minutes (min 1)
func handleGCCatalogCache(
	proc *process.Process,
	service serviceType,
	parameter string,
	_ requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewNotSupportedf(
			proc.Ctx, "GCCatalogCache only supports CN service")
	}

	ago := 20 * time.Minute
	if p := strings.TrimSpace(parameter); p != "" {
		minutes, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return Result{}, moerr.NewInvalidInput(
				proc.Ctx, "invalid minutes: "+p)
		}
		if minutes < 1 {
			minutes = 1
		}
		ago = time.Duration(minutes) * time.Minute
	}

	eng := proc.GetSessionInfo().StorageEngine
	gcer, ok := eng.(engine.CatalogCacheGCer)
	if !ok {
		return Result{
			Method: GCCatalogCacheMethod,
			Data:   "not supported by engine",
		}, nil
	}

	gcer.GCCatalogCache(proc.Ctx, ago)

	return Result{
		Method: GCCatalogCacheMethod,
		Data:   fmt.Sprintf("GC catalog cache entries older than %v done", ago),
	}, nil
}
