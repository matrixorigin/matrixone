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

package write

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type TableCache interface {
	InvalidateTable(accountID uint32, catalogID uint64, namespace, table string) int
}

type RemoteCacheInvalidator interface {
	InvalidateIcebergTable(ctx context.Context, req api.AppendRequest, result api.CommitResult) error
}

type RemoteCacheInvalidatorFunc func(ctx context.Context, req api.AppendRequest, result api.CommitResult) error

func (f RemoteCacheInvalidatorFunc) InvalidateIcebergTable(ctx context.Context, req api.AppendRequest, result api.CommitResult) error {
	return f(ctx, req, result)
}

type MetadataCacheInvalidator struct {
	Cache  TableCache
	Remote RemoteCacheInvalidator
}

func (i MetadataCacheInvalidator) InvalidateIcebergTable(ctx context.Context, req api.AppendRequest, result api.CommitResult) error {
	if i.Cache != nil {
		i.Cache.InvalidateTable(req.Catalog.AccountID, req.Catalog.CatalogID, strings.Join(req.Namespace, "."), req.Table)
	}
	if i.Remote != nil {
		_ = i.Remote.InvalidateIcebergTable(ctx, req, result)
	}
	return nil
}
