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
	"context"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
)

const (
	BootstrapInterval = time.Millisecond * 200
	BootstrapTimeout  = time.Minute * 5
)

func (h *handler) bootstrap(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()
	retry := 0
	getClient := func() util.HAKeeperClient {
		return h.haKeeperClient
	}
	var state pb.CheckerState
	var err error
	for retry < int(BootstrapTimeout/BootstrapInterval) {
		select {
		case <-ticker.C:
			func(ctx context.Context) {
				ctx, cancel := context.WithTimeout(ctx, time.Second*3)
				defer cancel()
				state, err = h.haKeeperClient.GetClusterState(ctx)
				if err != nil {
					panic(err)
				}
			}(ctx)
			if state.TaskTableUser.GetUsername() != "" && state.TaskTableUser.GetPassword() != "" {
				db_holder.SetSQLWriterDBUser(db_holder.MOLoggerUser, state.TaskTableUser.GetPassword())
				db_holder.SetSQLWriterDBAddressFunc(util.AddressFunc(getClient))
				h.sqlWorker.SetSQLUser(SQLUsername, state.TaskTableUser.GetPassword())
				h.sqlWorker.SetAddressFn(util.AddressFunc(getClient))
				return
			}
		case <-ctx.Done():
			return
		}
		retry += 1
	}
	panic("proxy bootstrap failed")
}
