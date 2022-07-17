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

import "github.com/matrixorigin/matrixone/pkg/txn/clock"

const (
	memStorageEngine    = "MEM"
	taeStorageEngine    = "TAE"
	sqliteStorageEngine = "SQLITE"

	localClockSource = "LOCAL"
	hlcClockSource   = "HLC"

	memFileServiceBackend  = "MEM"
	diskFileServiceBackend = "DISK"
	s3FileServiceBackend   = "S3"
)

var (
	supportTxnStorageEngines = map[string]struct{}{
		memStorageEngine:    {},
		taeStorageEngine:    {},
		sqliteStorageEngine: {},
	}

	supportTxnClockSources = map[string]struct{}{
		localClockSource: {},
		hlcClockSource:   {},
	}

	supportFileServiceBackends = map[string]struct{}{
		memFileServiceBackend:  {},
		diskFileServiceBackend: {},
		s3FileServiceBackend:   {},
	}
)

func (s *store) createMemClock() {
	s.clock = clock.NewUnixNanoHLCClockWithStopper(s.stopper, s.cfg.Txn.Clock.MaxClockOffset.Duration)
}

// func init() {
// 	initMem()
// 	initLocalClock()

// 	// TODO: init hlc clock and TAE storage
// }

// func initMem() {
// 	storageFactories["MEM"] = func(shard metadata.DNShard, cfg *Config, logClient logservice.Client) (storage.TxnStorage, error) {
// 		return mem.NewKVTxnStorage(0, logClient), nil
// 	}
// }

// func initLocalClock() {
// 	clockFactories["LOCAL"] = func(stopper *stopper.Stopper, cfg *Config) clock.Clock {
// 		return clock.NewUnixNanoHLCClockWithStopper(stopper, cfg.Txn.Clock.MaxClockOffset.Duration)
// 	}
// }
