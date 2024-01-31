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

package task

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type rule func(pb.CNStoreInfo) bool

func containsLabel(key, label string) func(info pb.CNStoreInfo) bool {
	return func(info pb.CNStoreInfo) bool {
		return contains(info.Labels[key].Labels, label)
	}
}

func notContainsLabel(key, label string) func(info pb.CNStoreInfo) bool {
	return func(info pb.CNStoreInfo) bool {
		return !contains(info.Labels[key].Labels, label)
	}
}

func withMemory(requirement uint64) func(info pb.CNStoreInfo) bool {
	return func(info pb.CNStoreInfo) bool {
		memTotal := info.Resource.MemTotal
		if memTotal > requirement {
			return true
		}
		return false
	}
}

func notExpired(cfg hakeeper.Config, currentTick uint64) func(info pb.CNStoreInfo) bool {
	return func(info pb.CNStoreInfo) bool {
		return !cfg.CNStoreExpired(info.Tick, currentTick)
	}
}

func matchAllRules(cn pb.CNStoreInfo, rules ...rule) bool {
	for _, rule := range rules {
		if !rule(cn) {
			return false
		}
	}
	return true
}

func selectCNs(cnState pb.CNState, rules ...rule) (uuids []string) {
	for uuid, cn := range cnState.Stores {
		if matchAllRules(cn, rules...) {
			uuids = append(uuids, uuid)
		}
	}
	return uuids
}

func contains(slice []string, val string) bool {
	for _, v := range slice {
		if strings.EqualFold(val, v) {
			return true
		}
	}
	return false
}
