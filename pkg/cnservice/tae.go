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
	"os"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

func initTAE(
	cancelMoServerCtx context.Context,
	pu *config.ParameterUnit,
	cfg *Config,
) error {

	targetDir := pu.SV.StorePath

	mask := syscall.Umask(0)
	if err := os.MkdirAll(targetDir, os.FileMode(0755)); err != nil {
		syscall.Umask(mask)
		logutil.Infof("Recreate dir error:%v\n", err)
		return err
	}
	syscall.Umask(mask)

	opts := &options.Options{}
	switch cfg.Engine.Logstore {
	case options.LogstoreLogservice:
		lc := func() (logservice.Client, error) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			lc, err := logservice.NewClient(ctx, logservice.ClientConfig{
				ReadOnly:         false,
				LogShardID:       pu.SV.LogShardID,
				DNReplicaID:      pu.SV.DNReplicaID,
				ServiceAddresses: cfg.HAKeeper.ClientConfig.ServiceAddresses,
			})
			cancel()
			return lc, err
		}
		opts.Lc = lc
		opts.LogStoreT = options.LogstoreLogservice
	case options.LogstoreBatchStore, "":
		opts.LogStoreT = options.LogstoreBatchStore
	default:
		return moerr.NewInternalError("invalid logstore type: %v", cfg.Engine.Logstore)
	}

	tae, err := db.Open(targetDir+"/tae", opts)
	if err != nil {
		logutil.Infof("Open tae failed. error:%v", err)
		return err
	}

	eng := moengine.NewEngine(tae)
	pu.StorageEngine = eng
	pu.TxnClient = moengine.EngineToTxnClient(eng)
	logutil.Info("Initialize the engine Done")

	return nil
}
