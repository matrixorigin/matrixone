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

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
)

func initTAE(
	cancelMoServerCtx context.Context,
	pu *config.ParameterUnit,
) error {

	targetDir := pu.SV.StorePath

	mask := syscall.Umask(0)
	if err := os.MkdirAll(targetDir, os.FileMode(0755)); err != nil {
		syscall.Umask(mask)
		logutil.Infof("Recreate dir error:%v\n", err)
		return err
	}
	syscall.Umask(mask)

	tae, err := db.Open(targetDir+"/tae", nil)
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
