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

package testtxnengine

import (
	"context"
	"math"
	"time"

	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

type testEnv struct {
	txnClient client.TxnClient
	engine    *txnengine.Engine
	nodes     []*Node
	clock     clock.Clock
	sender    *Sender
}

func (t *testEnv) Close() error {
	for _, node := range t.nodes {
		if err := node.service.Close(true); err != nil {
			return err
		}
	}
	return nil
}

func newEnv() *testEnv {
	env := &testEnv{}

	sender := &Sender{
		env: env,
	}
	env.sender = sender

	env.txnClient = client.NewTxnClient(sender)

	env.clock = clock.NewHLCClock(
		func() int64 {
			return time.Now().Unix()
		},
		math.MaxInt64,
	)

	env.nodes = []*Node{
		env.NewNode(1),
	}

	env.engine = txnengine.New(
		context.Background(),
		env,
		func() (details logservicepb.ClusterDetails, err error) {
			for _, node := range env.nodes {
				details.DNNodes = append(details.DNNodes, node.info)
			}
			return
		},
	)

	return env
}
