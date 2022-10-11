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

package cnservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *service) initDistributedTAE(
	ctx context.Context,
	pu *config.ParameterUnit,
) error {

	// txn client
	client, err := s.getTxnClient()
	if err != nil {
		return err
	}
	pu.TxnClient = client

	// hakeeper
	hakeeper, err := s.getHAKeeperClient()
	if err != nil {
		return err
	}

	txnOperator, err := pu.TxnClient.New()
	if err != nil {
		return err
	}

	// Should be no fixed or some size?
	mp, err := mpool.NewMPool("distributed_tae", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	proc := process.New(ctx, mp, pu.TxnClient, txnOperator, pu.FileService)

	// engine
	pu.StorageEngine = disttae.New(
		proc,
		ctx,
		client,
		hakeeper,
		memoryengine.GetClusterDetailsFromHAKeeper(
			ctx,
			hakeeper,
		),
	)

	return nil
}
