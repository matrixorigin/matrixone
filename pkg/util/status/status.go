// Copyright 2021 -2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package status

type Status struct {
	LogtailServerStatus LogtailServerStatus  `json:"logtail_server_status"`
	HAKeeperStatus      HAKeeperStatus       `json:"hakeeper_status"`
	CNUUIDStatus        map[string]*CNStatus `json:"cn_uuid_status"`
}

type CNStatus struct {
	TxnClientStatus TxnClientStatus `json:"txn_client_status"`
	LockStatus      LockStatus      `json:"lock_status"`
	LogtailStatus   LogtailStatus   `json:"logtail_status"`
}

func (s *Status) fillCNStatus(cnInstances map[string]*CNInstance) {
	s.CNUUIDStatus = make(map[string]*CNStatus, len(cnInstances))
	for cn, instance := range cnInstances {
		cnStatus := &CNStatus{}
		s.CNUUIDStatus[cn] = cnStatus
		cnStatus.TxnClientStatus.fill(instance.TxnClient)
		cnStatus.LockStatus.fill(instance.LockService)
		cnStatus.LogtailStatus.fill(instance.logtailClient)
	}
}
