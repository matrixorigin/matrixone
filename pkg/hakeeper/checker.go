// Copyright 2021 - 2022 Matrix Origin
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

package hakeeper

// FIXME: placeholder, please feel free to change/remove.
type Operator struct {
	// UUID of the store that suppose to consume this Operator
	UUID string
	// other fields might be opCode, payload etc.
	// feel free to add whatever you need
}

// Checker is the interface suppose to be implemented by HAKeeper's
// coordinator. Checker is suppose to be stateless - Checker is free to
// maintain whatever internal states, but those states should never be
// assumed to be persistent across reboots.
type Checker interface {
	// Check is periodically called by the HAKeeper for checking the cluster
	// health status, a list of Operator instances will be returned describing
	// actions required to ensure the high availability of the cluster.
	Check(cluster ClusterInfo,
		dn DNState, log LogState, currentTick uint64) []Operator
}
