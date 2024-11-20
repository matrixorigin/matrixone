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

package v2_0_0

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
)

func TestUpgradePubSub(t *testing.T) {
	getAccountsStub := gostub.Stub(&pubsub.GetAccounts, func(_ executor.TxnExecutor) (map[string]*pubsub.AccountInfo, map[int32]*pubsub.AccountInfo, error) {
		return map[string]*pubsub.AccountInfo{
			"accName": {Id: 0, Name: "accName"},
		}, nil, nil
	})
	defer getAccountsStub.Reset()

	getAllPubInfosStub := gostub.Stub(&versions.GetAllPubInfos, func(_ executor.TxnExecutor, _ map[string]*pubsub.AccountInfo) (map[string]*pubsub.PubInfo, error) {
		return map[string]*pubsub.PubInfo{
			"accName#pubName": {PubName: "pubName", SubAccountsStr: "accName"},
		}, nil
	})
	defer getAllPubInfosStub.Reset()

	getPubSubscribedInfosStub := gostub.Stub(&getPubSubscribedInfos, func(_ executor.TxnExecutor) (map[string][]*pubsub.SubInfo, error) {
		return nil, nil
	})
	defer getPubSubscribedInfosStub.Reset()

	err := UpgradePubSub(&MockTxnExecutor{})
	assert.NoError(t, err)
}
