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

package cnservice

import (
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

type cnServiceChecker struct {
	hakeeper.CheckerCommonFields
	cnState pb.CNState
}

func NewCNServiceChecker(
	commonFields hakeeper.CheckerCommonFields,
	cnState pb.CNState,
) hakeeper.ModuleChecker {
	return &cnServiceChecker{
		CheckerCommonFields: commonFields,
		cnState:             cnState,
	}
}

func (c *cnServiceChecker) Check() (operators []*operator.Operator) {
	if c.User.Username == "" {
		runtime.ServiceRuntime(c.ServiceID).Logger().Warn("username is still empty.")
		return
	}
	working, expired := parseCNStores(c.Cfg, c.cnState, c.CurrentTick)
	if len(working)+len(expired) == 0 {
		runtime.ServiceRuntime(c.ServiceID).Logger().Error("there are no CNs yet.")
		return
	}
	for _, store := range working {
		if !c.cnState.Stores[store].TaskServiceCreated {
			runtime.ServiceRuntime(c.ServiceID).Logger().Info("create task service for CN.",
				zap.String("uuid", store))
			operators = append(operators, operator.CreateTaskServiceOp("",
				store, pb.CNService, c.User))
		}
		// If this instance has not joined gossip cluster, we generate join command.
		if !c.cnState.Stores[store].GossipJoined {
			addresses := getGossipAddresses(c.Cfg, c.cnState, c.CurrentTick, store)
			if len(addresses) > 0 {
				runtime.ServiceRuntime(c.ServiceID).Logger().Info("join gossip cluster for CN",
					zap.String("uuid", store),
					zap.Any("addresses", addresses))
				operators = append(operators, operator.JoinGossipClusterOp("",
					store, addresses))
			}
		}
	}
	for _, store := range expired {
		runtime.ServiceRuntime(c.ServiceID).Logger().Warn("expired CN.",
			zap.String("uuid", store))
		operators = append(operators, operator.CreateDeleteCNOp("", store))
	}
	return operators
}

// parseCNStores returns all expired stores' ids.
func parseCNStores(cfg hakeeper.Config, infos pb.CNState, currentTick uint64) ([]string, []string) {
	working := make([]string, 0)
	expired := make([]string, 0)
	for uuid, storeInfo := range infos.Stores {
		if cfg.CNStoreExpired(storeInfo.Tick, currentTick) {
			expired = append(expired, uuid)
		} else {
			working = append(working, uuid)
		}
	}

	return working, expired
}

// getGossipAddresses returns the gossip addresses of CN stores that are in working state.
func getGossipAddresses(cfg hakeeper.Config, infos pb.CNState, currentTick uint64, self string) []string {
	var addresses []string
	var count int
	for uuid, storeInfo := range infos.Stores {
		if !cfg.CNStoreExpired(storeInfo.Tick, currentTick) && uuid != self {
			if len(storeInfo.GossipAddress) > 0 {
				addresses = append(addresses, storeInfo.GossipAddress)
				count++
				if count > 2 {
					break
				}
			}
		}
	}
	return addresses
}
