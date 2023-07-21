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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
)

const (
	BootstrapInterval = time.Millisecond * 200
	BootstrapTimeout  = time.Minute * 5
)

func (h *handler) bootstrap(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	state, err := h.haKeeperClient.GetClusterState(ctx)
	if err != nil {
		panic(err)
	}
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()
	retry := 0
	for retry < int(BootstrapTimeout/BootstrapInterval) {
		select {
		case <-ticker.C:
			if state.TaskTableUser.GetUsername() != "" && state.TaskTableUser.GetPassword() != "" {
				db_holder.SetSQLWriterDBUser(db_holder.MOLoggerUser, state.TaskTableUser.GetPassword())
				h.sqlWorker.SetSQLUser(SQLUserName, state.TaskTableUser.GetPassword())

				addressFunc := func(ctx context.Context, _ bool) (string, error) {
					ctx, cancel := context.WithTimeout(ctx, time.Second*3)
					defer cancel()
					state, err := h.haKeeperClient.GetClusterState(ctx)
					if err != nil {
						return "", moerr.NewInvalidState(ctx, fmt.Sprintf("failed to get cluster state: %s", err.Error()))
					}
					if len(state.CNState.Stores) == 0 {
						return "", moerr.NewInvalidState(ctx, "no cn in the cluster")
					}
					for uuid := range state.CNState.Stores {
						return state.CNState.Stores[uuid].SQLAddress, nil
					}
					return "", nil
				}
				db_holder.SetSQLWriterDBAddressFunc(addressFunc)
				h.sqlWorker.SetAddressFn(addressFunc)

				h.logger.Info("proxy bootstrap succeeded")
				return
			}
		case <-ctx.Done():
			h.logger.Info("proxy bootstrap interrupt for context done.")
			return
		}
		retry += 1
	}
	panic("proxy bootstrap failed")
}
