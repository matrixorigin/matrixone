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

package iceberg

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

type QueryMessageClient interface {
	ServiceID() string
	NewRequest(query.CmdMethod) *query.Request
	SendMessage(context.Context, string, *query.Request) (*query.Response, error)
	Release(*query.Response)
}

type ClusterRemoteCacheInvalidator struct {
	Cluster     clusterservice.MOCluster
	QueryClient QueryMessageClient
	Timeout     time.Duration
}

func (i ClusterRemoteCacheInvalidator) InvalidateIcebergTable(ctx context.Context, req api.AppendRequest, result api.CommitResult) error {
	if i.Cluster == nil || i.QueryClient == nil {
		return nil
	}
	timeout := i.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	// Invalidation runs after the catalog commit has succeeded. Preserve values
	// from the statement context, but do not let a simultaneous caller cancel
	// suppress the post-commit notification; the independent timeout still
	// bounds the complete cluster broadcast.
	broadcastCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), timeout, moerr.CauseIcebergInternal)
	defer cancel()
	payload := query.IcebergCacheInvalidateRequest{
		AccountID:            req.Catalog.AccountID,
		CatalogID:            req.Catalog.CatalogID,
		Namespace:            api.NamespaceCacheKey(req.Namespace),
		Table:                req.Table,
		SnapshotID:           result.SnapshotID,
		MetadataLocationHash: result.MetadataLocationHash,
		CommitID:             result.CommitID,
	}
	self := i.QueryClient.ServiceID()
	_ = clusterservice.GetCNServiceWithoutWorkingStateWithContext(broadcastCtx, i.Cluster, clusterservice.NewSelector(), func(cn metadata.CNService) bool {
		if cn.WorkState != metadata.WorkState_Working && cn.WorkState != metadata.WorkState_Unknown {
			return true
		}
		if strings.TrimSpace(cn.QueryAddress) == "" || (self != "" && cn.ServiceID == self) {
			return true
		}
		request := i.QueryClient.NewRequest(query.CmdMethod_IcebergCacheInvalidate)
		request.IcebergCacheInvalidateRequest = payload
		resp, err := i.QueryClient.SendMessage(broadcastCtx, cn.QueryAddress, request)
		if err == nil && resp != nil {
			i.QueryClient.Release(resp)
		}
		return broadcastCtx.Err() == nil
	})
	return nil
}
