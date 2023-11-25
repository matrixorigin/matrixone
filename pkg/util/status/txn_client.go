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

import (
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type TxnClientStatus struct {
	// indicate whether the CN can provide service normally.
	// 0 means paused, 1 means normal.
	State int
	// number of user active transactions.
	UserTxnNum int
	// all active txns
	ActiveTxns     []string
	ActiveTxnCount int
	// FIFO queue for ready to active txn
	WaitActiveTxns     []string
	WaitActiveTxnCount int
	// LatestTS is the latest TS for the txn client.
	LatestTS timestamp.Timestamp
}

func (s *Status) fillTxnClient(cnList []CNInstance) {
	s.CNStatus = s.CNStatus[:0]
	for _, cn := range cnList {
		st := cn.TxnClient.GetState()
		s.CNStatus = append(s.CNStatus, CNStatus{
			UUID: cn.UUID,
			TxnClientStatus: TxnClientStatus{
				State:              st.State,
				UserTxnNum:         st.Users,
				ActiveTxns:         st.ActiveTxns,
				ActiveTxnCount:     len(st.ActiveTxns),
				WaitActiveTxns:     st.WaitActiveTxns,
				WaitActiveTxnCount: len(st.WaitActiveTxns),
				LatestTS:           st.LatestTS,
			},
		})

	}
}
