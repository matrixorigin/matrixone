// Copyright 2023 Matrix Origin
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

package clusterservice

import (
	"context"

	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// Op compare op
type Op int

var (
	// EQ ==
	EQ = Op(1)
)

// Selector is used to choose a service from MOCluster
type Selector struct {
	byServiceID bool
	serviceID   string

	byLabel bool
	labels  map[string]string
	labelOp Op

	// all is ture means that we only want all the CN services to do
	// the filter. Otherwise, only the ones whose work state is working
	// would do the filter. This is only used for CN. Its default value
	// is false.
	all bool
}

// MOCluster is used to get the meta and status information of the MO cluster.
//
// TODO(fagongzi): In the future, all cluster-related information should be obtained
// from this interface, such as the distribution of table data on cn and other
// statistical information.
type MOCluster interface {
	// GetCNService get services by selector, and the applyFunc used to save the
	// cn service that matches the selector's conditions.
	//
	// Since the query result may be a Slice, to avoid memory allocation overhead,
	// we use apply to notify the caller of a Service that satisfies the condition.
	GetCNService(selector Selector, apply func(metadata.CNService) bool)
	// GetTNService get services by selector, and the applyFunc used to save the
	// tn service that matches the selector's conditions.
	//
	// Since the query result may be a Slice, to avoid memory allocation overhead,
	// we use apply to notify the caller of a Service that satisfies the condition.
	GetTNService(selector Selector, apply func(metadata.TNService) bool)
	// GetCNServiceWithoutWorkingState get services by selector, and the applyFunc used to save the
	// cn service that matches the selector's conditions.
	//
	// Since the query result may be a Slice, to avoid memory allocation overhead,
	// we use apply to notify the caller of a Service that satisfies the condition.
	GetCNServiceWithoutWorkingState(selector Selector, apply func(metadata.CNService) bool)
	// ForceRefresh when other modules use the cluster information and find out that
	// the current cache information is out of date, you can force the cache to be
	// refreshed.
	ForceRefresh(sync bool)
	// Close close the cluster
	Close()
	// DebugUpdateCNLabel updates the labels on specified CN. It is only used in mo_ctl
	// internally for debug purpose.
	DebugUpdateCNLabel(uuid string, kvs map[string][]string) error
}

type ClusterClient interface {
	GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error)
}

type labelSupportedClient interface {
	ClusterClient
	UpdateCNLabel(ctx context.Context, label logpb.CNStoreLabel) error
}
