// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type proxyServiceChecker struct {
	hakeeper.CheckerCommonFields
	proxyState pb.ProxyState
}

func NewProxyServiceChecker(
	commonFields hakeeper.CheckerCommonFields,
	proxyState pb.ProxyState,
) hakeeper.ModuleChecker {
	return &proxyServiceChecker{
		CheckerCommonFields: commonFields,
		proxyState:          proxyState,
	}
}

func (c *proxyServiceChecker) Check() (operators []*operator.Operator) {
	_, expired := parseProxyStores(c.Cfg, c.proxyState, c.CurrentTick)
	for _, uuid := range expired {
		operators = append(operators, operator.CreateDeleteProxyOp("", uuid))
	}
	return operators
}

// parseProxyStores returns all working and expired stores' ids.
func parseProxyStores(cfg hakeeper.Config, infos pb.ProxyState, currentTick uint64) ([]string, []string) {
	working := make([]string, 0)
	expired := make([]string, 0)
	for uuid, storeInfo := range infos.Stores {
		if cfg.ProxyStoreExpired(storeInfo.Tick, currentTick) {
			expired = append(expired, uuid)
		} else {
			working = append(working, uuid)
		}
	}
	return working, expired
}
