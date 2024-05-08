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

package txn

var (
	txnFeatureUserTxn      = uint32(1 << 0)
	txnFeatureReadOnly     = uint32(1 << 1)
	txnFeatureCacheWrite   = uint32(1 << 2)
	txnFeatureDisable1PC   = uint32(1 << 3)
	txnFeatureCheckDup     = uint32(1 << 4)
	txnFeatureDisableTrace = uint32(1 << 5)
)

func (m TxnOptions) WithDisableTrace() TxnOptions {
	m.Features |= txnFeatureDisableTrace
	return m
}

func (m TxnOptions) WithEnableCheckDup() TxnOptions {
	m.Features |= txnFeatureCheckDup
	return m
}

func (m TxnOptions) WithDisable1PC() TxnOptions {
	m.Features |= txnFeatureDisable1PC
	return m
}

func (m TxnOptions) WithEnableCacheWrite() TxnOptions {
	m.Features |= txnFeatureCacheWrite
	return m
}

func (m TxnOptions) WithReadOnly() TxnOptions {
	m.Features |= txnFeatureReadOnly
	return m
}

func (m TxnOptions) WithUserTxn() TxnOptions {
	m.Features |= txnFeatureUserTxn
	return m
}

func (m TxnOptions) CacheWriteEnabled() bool {
	return m.featureEnabled(txnFeatureCacheWrite)
}

func (m TxnOptions) TraceDisabled() bool {
	return m.featureEnabled(txnFeatureDisableTrace)
}

func (m TxnOptions) UserTxn() bool {
	return m.featureEnabled(txnFeatureUserTxn)
}

func (m TxnOptions) ReadOnly() bool {
	return m.featureEnabled(txnFeatureReadOnly)
}

func (m TxnOptions) CheckDupEnabled() bool {
	return m.featureEnabled(txnFeatureCheckDup)
}

func (m TxnOptions) Is1PCDisabled() bool {
	return m.featureEnabled(txnFeatureDisable1PC)
}

func (m TxnOptions) featureEnabled(feature uint32) bool {
	return m.Features&feature > 0
}
