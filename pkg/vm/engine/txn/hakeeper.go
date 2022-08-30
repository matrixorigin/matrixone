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

package txnengine

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func GetClusterDetailsFromHAKeeper(
	ctx context.Context,
	client logservice.CNHAKeeperClient,
) (
	get GetClusterDetailsFunc,
) {

	var lock sync.Mutex
	var detailsError error
	var details logservicepb.ClusterDetails

	update := func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second*17)
		defer cancel()
		ret, err := client.GetClusterDetails(ctx)
		lock.Lock()
		defer lock.Unlock()
		detailsError = err
		details = ret
		logutil.Info("txnengine: cluster details updated")
	}
	update()

	go func() {

		ticker := time.NewTicker(time.Second * 17)
		for {
			select {

			case <-ticker.C:
				update()

			case <-ctx.Done():
				return

			}
		}
	}()

	get = func() (logservicepb.ClusterDetails, error) {
		lock.Lock()
		defer lock.Unlock()
		return details, detailsError
	}

	return
}
