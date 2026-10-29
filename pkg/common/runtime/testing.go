// Copyright 2021-2024 Matrix Origin
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

package runtime

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

func InTesting(
	sid string,
) bool {
	_, ok := ServiceRuntime(sid).GetGlobalVariables(TestingContextKey)
	return ok
}

func SetupServiceRuntimeTestingContext(
	sid string,
) {
	ServiceRuntime(sid).SetGlobalVariables(TestingContextKey, &TestingContext{})
}

func MustGetTestingContext(
	sid string,
) *TestingContext {
	v, ok := ServiceRuntime(sid).GetGlobalVariables(TestingContextKey)
	if !ok {
		panic("testing context not found")
	}
	return v.(*TestingContext)
}

type TestingContext struct {
	sync.RWMutex

	adjustLockResultFunc func(txnID []byte, tableID uint64, res *lock.Result)
	beforeLockFunc       func(txnID []byte, tableID uint64)
	loadDataConfig       LoadDataTestConfig
	txnModeConfig        TxnModeTestConfig
}

// LoadDataTestConfig lets an embedded SQL test cross the parallel reader
// admission and local worker thresholds with a small file. It applies only to
// one service and exact path.
// OnRanges observes a private copy of actual split offsets after planning.
type LoadDataTestConfig struct {
	Filepath string
	MinSize  int64
	OnRanges func(readers int, offsets []int64)
}

// SetLoadDataTestConfig returns the previous configuration for scoped restore.
func (tc *TestingContext) SetLoadDataTestConfig(cfg LoadDataTestConfig) LoadDataTestConfig {
	tc.Lock()
	defer tc.Unlock()
	previous := tc.loadDataConfig
	tc.loadDataConfig = cfg
	return previous
}

// GetLoadDataTestConfig is inert outside a registered testing runtime. Copy the
// configuration under the lock; callers must invoke observers after it returns.
func GetLoadDataTestConfig(service, filepath string) (LoadDataTestConfig, bool) {
	if service == "" {
		return LoadDataTestConfig{}, false
	}
	rt := ServiceRuntime(service)
	if rt == nil {
		return LoadDataTestConfig{}, false
	}
	v, ok := rt.GetGlobalVariables(TestingContextKey)
	if !ok {
		return LoadDataTestConfig{}, false
	}
	tc := v.(*TestingContext)
	tc.RLock()
	cfg := tc.loadDataConfig
	tc.RUnlock()
	return cfg, cfg.Filepath != "" && cfg.Filepath == filepath && cfg.MinSize > 0
}

func (tc *TestingContext) SetAdjustLockResultFunc(
	fn func(txnID []byte, tableID uint64, res *lock.Result),
) {
	tc.Lock()
	defer tc.Unlock()
	tc.adjustLockResultFunc = fn
}

func (tc *TestingContext) GetAdjustLockResultFunc() func(txnID []byte, tableID uint64, res *lock.Result) {
	tc.RLock()
	defer tc.RUnlock()
	if tc.adjustLockResultFunc == nil {
		return func(txnID []byte, tableID uint64, res *lock.Result) {}
	}
	return tc.adjustLockResultFunc
}

func (tc *TestingContext) SetBeforeLockFunc(
	fn func(txnID []byte, tableID uint64),
) {
	tc.Lock()
	defer tc.Unlock()
	tc.beforeLockFunc = fn
}

func (tc *TestingContext) GetBeforeLockFunc() func(txnID []byte, tableID uint64) {
	tc.RLock()
	defer tc.RUnlock()
	if tc.adjustLockResultFunc == nil {
		return func(txnID []byte, tableID uint64) {}
	}
	return tc.beforeLockFunc
}

// TxnModeTestConfig changes transaction mode only for a test's own account.
// Session isolation remains controlled by SQL; zero configuration is inert.
type TxnModeTestConfig struct {
	AccountID uint32
	Mode      txn.TxnMode
	Enabled   bool
}

// SetTxnModeTestConfig returns the previous configuration for scoped restore.
func (tc *TestingContext) SetTxnModeTestConfig(cfg TxnModeTestConfig) TxnModeTestConfig {
	tc.Lock()
	defer tc.Unlock()
	previous := tc.txnModeConfig
	tc.txnModeConfig = cfg
	return previous
}

// GetTxnModeTestConfig leaves ordinary runtimes and unrelated accounts unchanged.
func GetTxnModeTestConfig(service string, accountID uint32) (txn.TxnMode, bool) {
	rt := ServiceRuntime(service)
	if rt == nil {
		return 0, false
	}
	v, ok := rt.GetGlobalVariables(TestingContextKey)
	if !ok {
		return 0, false
	}
	tc := v.(*TestingContext)
	tc.RLock()
	cfg := tc.txnModeConfig
	tc.RUnlock()
	return cfg.Mode, cfg.Enabled && cfg.AccountID == accountID
}
