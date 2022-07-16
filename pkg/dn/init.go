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

package dn

import (
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
)

var (
	storageFactories = map[string]storageFactory{}
	clockFactories   = map[string]func(stopper *stopper.Stopper, cfg *Config) clock.Clock{}
)

func init() {
	initMem()
	initLocalClock()

	// TODO: init hlc clock and TAE storage
}

func initMem() {
	storageFactories["MEM"] = func(shard metadata.DNShard, cfg *Config, logClient logservice.Client) (storage.TxnStorage, error) {
		return mem.NewKVTxnStorage(0, logClient), nil
	}
}

func initLocalClock() {
	clockFactories["LOCAL"] = func(stopper *stopper.Stopper, cfg *Config) clock.Clock {
		return clock.NewUnixNanoHLCClockWithStopper(stopper, cfg.Txn.MaxClockOffset.Duration)
	}
}
