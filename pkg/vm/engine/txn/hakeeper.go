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
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"time"
)

func (e *Engine) startHAKeeperLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 17)
	for {
		select {

		case <-ticker.C:
			if err := e.updateClusterDetails(ctx); err != nil {
				// lost contact with hakeeper
				e.fatal()
				return
			}

		case <-ctx.Done():
			return

		}
	}
}

func (e *Engine) updateClusterDetails(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*17)
	defer cancel()
	details, err := e.hakeeperClient.GetClusterDetails(ctx)
	if err != nil {
		return err
	}
	e.clusterDetails.Lock()
	defer e.clusterDetails.Unlock()
	e.clusterDetails.ClusterDetails = details
	return nil
}

func (e *Engine) getDataNodes() []logservicepb.DNNode {
	e.clusterDetails.Lock()
	defer e.clusterDetails.Unlock()
	return e.clusterDetails.DNNodes
}

func (e *Engine) getComputeNodes() []logservicepb.CNNode {
	e.clusterDetails.Lock()
	defer e.clusterDetails.Unlock()
	return e.clusterDetails.CNNodes
}
