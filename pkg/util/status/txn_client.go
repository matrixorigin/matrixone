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
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type TxnClientStatus struct {
	// indicate whether the CN can provide service normally.
	// 0 means paused, 1 means normal.
	State int `json:"state"`
	// number of user active transactions.
	UserTxnNum int `json:"user_txn_num"`
	// all active txns
	ActiveTxns     []string `json:"active_txns"`
	ActiveTxnCount int      `json:"active_txn_count"`
	// FIFO queue for ready to active txn
	WaitActiveTxns     []string `json:"wait_active_txns"`
	WaitActiveTxnCount int      `json:"wait_active_txn_count"`
	// LatestTS is the latest TS for the txn client.
	LatestTS timestamp.Timestamp `json:"latest_ts"`
}

func (s *Status) fillTxnClient(cnStatus *CNStatus, txnClient client.TxnClient) {
	st := txnClient.GetState()
	cnStatus.TxnClientStatus = TxnClientStatus{
		State:              st.State,
		UserTxnNum:         st.Users,
		ActiveTxns:         st.ActiveTxns,
		ActiveTxnCount:     len(st.ActiveTxns),
		WaitActiveTxns:     st.WaitActiveTxns,
		WaitActiveTxnCount: len(st.WaitActiveTxns),
		LatestTS:           st.LatestTS,
	}
}
