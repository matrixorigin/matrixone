// Copyright 2022 Matrix Origin
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

package util

import (
	"context"
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// HAKeeperClient is an interface which is mainly used to avoid cycle import.
type HAKeeperClient interface {
	// GetClusterDetails queries the HAKeeper and return CN and TN nodes that are
	// known to the HAKeeper.
	GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error)
}

func AddressFunc(getClient func() HAKeeperClient) func(context.Context, bool) (string, error) {
	return func(ctx context.Context, random bool) (string, error) {
		if getClient == nil || getClient() == nil {
			return "", moerr.NewNoHAKeeper(ctx)
		}
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		selector := clusterservice.NewSelector().SelectByLabel(nil, clusterservice.EQ)
		if s, exist := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.BackgroundCNSelector); exist {
			selector = s.(clusterservice.Selector)
		}
		details, err := getClient().GetClusterDetails(ctx)
		if err != nil {
			return "", err
		}
		cns := make([]pb.CNStore, 0, len(details.CNStores))
		labeled_cns := make([]pb.CNStore, 0, len(details.CNStores))
		for _, cn := range details.CNStores {
			if cn.WorkState == metadata.WorkState_Working {
				cns = append(cns, cn)
				// get logging cn label name
				if cn.Labels != nil && selector.Match(cn.Labels) {
					labeled_cns = append(labeled_cns, cn)
				}
			}
		}
		if len(cns) == 0 {
			return "", moerr.NewInvalidState(ctx, "no CN in the cluster")
		}
		var selectedCNs []pb.CNStore // Replace CNType with the actual type of your CNs
		if len(labeled_cns) > 0 {
			selectedCNs = labeled_cns
		} else {
			selectedCNs = cns
		}

		var n int
		if random {
			n = rand.Intn(len(selectedCNs))
		} else {
			n = len(selectedCNs) - 1
		}

		return selectedCNs[n].SQLAddress, nil

	}
}
